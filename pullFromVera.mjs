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

const schema = process.env.VERA_SCHEMA;

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

    let body;
    let chainId;
    let address;
    try {
      // Get all FK information
      const { rows } = await veraClient.query(
        `
            SELECT 
                contract_deployments.chain_id,
                concat('0x', encode(contract_deployments.address, 'hex')) as address,
                json_build_object(
                  'language', INITCAP(compiled_contracts.language), 
                  'sources', json_object_agg(compiled_contracts_sources.path, json_build_object('content', sources.content)),
                  'settings', compiled_contracts.compiler_settings
                ) as std_json_input,
                compiled_contracts.version as compiler_version,
                compiled_contracts.fully_qualified_name,
                concat('0x', encode(contract_deployments.transaction_hash, 'hex')) as creation_transaction_hash
            FROM ${schema}.verified_contracts
              JOIN ${schema}.compiled_contracts ON compiled_contracts.id = verified_contracts.compilation_id
              JOIN ${schema}.contract_deployments ON contract_deployments.id = verified_contracts.deployment_id
              JOIN ${schema}.compiled_contracts_sources ON compiled_contracts_sources.compilation_id = compiled_contracts.id
              LEFT JOIN ${schema}.sources ON sources.source_hash = compiled_contracts_sources.source_hash
            WHERE
                verified_contracts.id = $1
            GROUP BY 
              verified_contracts.id, 
              compiled_contracts.id, 
              contract_deployments.id,
              contracts.id;
        `,
        [payload.id]
      );

      if (rows.length === 0) {
        logger.error("No contract found for the given verified_contract ID", {
          verifiedContractId: payload.id,
        });
        return;
      }

      chainId = rows[0].chain_id;
      address = rows[0].address;
      const stdJsonInput = rows[0].std_json_input;
      const compilerVersion = rows[0].compiler_version;
      const contractIdentifier = rows[0].fully_qualified_name;
      const creationTransactionHash = rows[0].creation_transaction_hash;

      // For some reason inside `compilation.compiler_settings` there is a compilationTarget parameter that is not supported by solc
      delete stdJsonInput.settings.compilationTarget;

      body = {
        stdJsonInput,
        compilerVersion,
        contractIdentifier,
        creationTransactionHash,
      };
    } catch (error) {
      logger.error("Error processing new_verified_contract notification", {
        message: error.message,
        errorObject: error,
        verifiedContractId: payload.id,
      });
      return;
    }

    logger.debug(
      `Contract's information fetched, calling sourcify-servefr /v2/verify/${chainId}/${address} with parameters`,
      body
    );
    try {
      const res = await fetch(
        `${process.env.SOURCIFY_SERVER_HOST}/v2/verify/${chainId}/${address}`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(body),
        }
      );

      if (res.status === 202) {
        const response = await res.json();
        logger.info("Contract successfully verified", {
          veraVerifiedContractId: payload.id,
          address: response.result[0].address,
          chainId: response.result[0].chainId,
          status: response.result[0].status,
        });
      } else {
        throw new Error(await res.json());
      }
    } catch (error) {
      logger.warn("Failed to submit contract for verification", {
        message: error.message,
        errorObject: error,
        verifiedContractId: payload.id,
      });
      return;
    }
  });

  subscriber.connect();
  logger.info("Started listening for VerA verified_contracts...");
  subscriber.listenTo("new_verified_contract");
}

main();
