import pg from "pg";
import createSubscriber from "pg-listen";
import dotenv from "dotenv";
import fetch from "node-fetch";
import logger from "./logger.mjs";

dotenv.config();
const { Client } = pg;

const veraClient = new Client({
  host: process.env.VERA_HOST,
  database: process.env.VERA_DB,
  user: process.env.VERA_USER,
  password: process.env.VERA_PASSWORD,
  port: process.env.VERA_PORT,
});

const subscriber = createSubscriber({
  host: process.env.VERA_HOST,
  port: parseInt(process.env.VERA_PORT) || 5432,
  database: process.env.VERA_DB,
  user: process.env.VERA_USER,
  password: process.env.VERA_PASSWORD,
});

async function main() {
  await veraClient.connect();

  subscriber.notifications.on("new_verified_contract", async (payload) => {
    logger.info("Received notification in 'new_verified_contract'", {
      veraVerifiedContractId: payload.id,
    });

    // Skip verified_contracts pushed by sourcify
    if (payload.created_by === "sourcify") {
      logger.info("Contract inserted by Sourcify, skipping.");
      return;
    }
    // Get all FK information
    const {
      rows: [deployment],
    } = await veraClient.query(
      "SELECT * FROM contract_deployments WHERE id = $1",
      [payload.deployment_id]
    );
    const {
      rows: [compilation],
    } = await veraClient.query(
      "SELECT * FROM compiled_contracts WHERE id = $1",
      [payload.compilation_id]
    );

    // For some reason inside `compilation.compiler_settings` there is a compilationTarget parameter that is not supported by solc
    const settings = compilation.compiler_settings;
    delete settings.compilationTarget;

    const settingsJson = JSON.stringify({
      language: "Solidity",
      sources: Object.keys(compilation.sources).reduce((obj, current) => {
        obj[current] = {
          content: compilation.sources[current],
        };
        return obj;
      }, {}),
      settings: compilation.compiler_settings,
    });

    logger.silly(
      "Contract's information fetched, calling sourcify-server /verify/solc-json with parameters",
      {
        veraVerifiedContractId: payload.id,
        address: "0x" + deployment.address.toString("hex"),
        chainId: deployment.chain_id,
        jsonInput: settingsJson,
      }
    );
    try {
      const res = await fetch(
        `${process.env.SOURCIFY_SERVER_HOST}/verify/solc-json`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            compilerVersion: compilation.version,
            contractName: compilation.name,
            address: "0x" + deployment.address.toString("hex"),
            chainId: deployment.chain_id,
            files: {
              "settings.json": settingsJson,
            },
          }),
        }
      );

      if (res.status === 200) {
        const response = await res.json();
        logger.info("Contract successfully verified", {
          veraVerifiedContractId: payload.id,
          address: response.result[0].address,
          chainId: response.result[0].chainId,
          status: response.result[0].status,
        });
      } else {
        throw new Error((await res.json()).error);
      }
    } catch (error) {
      logger.warn("Failed to verify contract with error", {
        error: error.message,
      });
    }
  });

  subscriber.connect();
  logger.info("Started listening for VerA verified_contracts...");
  subscriber.listenTo("new_verified_contract");
}

main();
