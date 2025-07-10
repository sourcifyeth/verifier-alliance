import pg from "pg";
import createSubscriber from "pg-listen";
import dotenv from "dotenv";
import fetch from "node-fetch";
import logger from "./logger.mjs";
import { AuthTypes, Connector } from "@google-cloud/cloud-sql-connector";

dotenv.config();
const { Client } = pg;

async function main() {
  logger.info("Starting VerA pulling sync service...");
  let veraClient;
  let subscriber;

  try {
    let clientConfig;
    if (process.env.ALLIANCE_GOOGLE_CLOUD_SQL_INSTANCE_NAME) {
      logger.info("Using Google Cloud SQL Connector");
      const connector = new Connector();
      const clientOpts = await connector.getOptions({
        instanceConnectionName:
          process.env.ALLIANCE_GOOGLE_CLOUD_SQL_INSTANCE_NAME,
        authType: AuthTypes.PASSWORD,
      });
      clientConfig = {
        ...clientOpts,
        user: process.env.ALLIANCE_GOOGLE_CLOUD_SQL_USER,
        database: process.env.ALLIANCE_GOOGLE_CLOUD_SQL_DATABASE,
        password: process.env.ALLIANCE_GOOGLE_CLOUD_SQL_PASSWORD,
      };
    } else {
      logger.info("Using PostgreSQL direct connection");
      clientConfig = {
        host: process.env.VERA_HOST,
        database: process.env.VERA_DB,
        user: process.env.VERA_USER,
        password: process.env.VERA_PASSWORD,
        port: process.env.VERA_PORT,
      };
    }
    logger.info("Done creating database client configuration");

    veraClient = new Client(clientConfig);
    logger.info("Created database client");

    subscriber = createSubscriber(clientConfig);
    logger.info("Created database subscriber");

    await veraClient.connect();
    logger.info("Connected to VerA database");
  } catch (error) {
    logger.error("Error creating database client", {
      message: error.message,
      errorObject: error,
    });
    process.exit(1);
  }

  const schema = process.env.VERA_SCHEMA;

  subscriber.notifications.on("new_verified_contract", async (payload) => {
    logger.info("Received notification in 'new_verified_contract'", {
      veraVerifiedContractId: payload.id,
    });

    // Skip verified_contracts pushed by sourcify
    // if (payload.created_by === "sourcify") {
    //   logger.info("Contract inserted by Sourcify, skipping.");
    //   return;
    // }

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
              contract_deployments.id
        `,
        [payload.id]
      );

      if (rows.length === 0) {
        logger.error("No contract found for the given verified_contract ID", {
          veraVerifiedContractId: payload.id,
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
        veraVerifiedContractId: payload.id,
      });
      return;
    }

    logger.debug(
      `Contract's information fetched, submitting to Sourcify server /v2/verify/${chainId}/${address} with parameters`,
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
        logger.info("Contract submitted for verification", {
          verificationId: response.verificationId,
        });
      } else {
        const errorResponse = await res.json();
        logger.warn(
          "Server returned an error when trying to submit for verification",
          {
            errorResponse,
            veraVerifiedContractId: payload.id,
          }
        );
        return;
      }
    } catch (error) {
      logger.warn("Failed to submit contract for verification", {
        message: error.message,
        errorObject: error,
        veraVerifiedContractId: payload.id,
      });
      return;
    }
  });
  logger.info("Created subscription handler for 'new_verified_contract'");

  // Graceful shutdown handling
  async function shutdown() {
    logger.info("Shutting down gracefully...");
    try {
      await subscriber.close();
      await veraClient.end();
      logger.info("Database connections closed successfully");
    } catch (error) {
      logger.error("Error during shutdown", { error: error.message });
      process.exit(1);
    }
    process.exit(0);
  }

  // Handle process termination signals
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  subscriber.events.on("error", (error) => {
    logger.error("Fatal database client error:", {
      error: JSON.stringify(error),
    });
    process.exit(1);
  });
  client.on("error", (error) => {
    logger.error("Fatal database client error:", {
      error: JSON.stringify(error),
    });
    process.exit(1);
  });

  try {
    await subscriber.connect();
    logger.info("Connected the subscriber to the database");

    await subscriber.listenTo("new_verified_contract");
    logger.info("Started listening for VerA verified_contracts...");
  } catch (error) {
    logger.error("Error starting subscriber", {
      message: error.message,
      errorObject: JSON.stringify(error),
    });
    process.exit(1);
  }
}

main();
