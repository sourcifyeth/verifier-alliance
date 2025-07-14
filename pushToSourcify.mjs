import pg from "pg";
import fs, { stat } from "fs";
import path from "path";
import dotenv from "dotenv";
import { AuthTypes, Connector } from "@google-cloud/cloud-sql-connector";

dotenv.config();

console.log("Starting Push to Sourcify verification");

const CURRENT_VERIFIED_CONTRACT_PATH =
  process.env.CURRENT_VERIFIED_CONTRACT_PATH;

if (!CURRENT_VERIFIED_CONTRACT_PATH) {
  throw new Error("CURRENT_VERIFIED_CONTRACT_PATH is not set");
}

// Load current verified contract counter from file
const COUNTER_FILE = path.join(
  CURRENT_VERIFIED_CONTRACT_PATH,
  "CURRENT_SOURCIFY_SYNC"
);
let CURRENT_VERIFIED_CONTRACT = 1;
if (fs.existsSync(COUNTER_FILE)) {
  CURRENT_VERIFIED_CONTRACT = parseInt(
    fs.readFileSync(COUNTER_FILE, "utf8"),
    10
  );
}

const N = 50; // Number of contracts to process at a time (smaller batch for API calls)

const VERA_SCHEMA = process.env.VERA_SCHEMA || "public";
const SOURCIFY_API_BASE = "https://sourcify.dev/server";

const VERA_DB_CONFIG = {
  database: process.env.VERA_DB,
  user: process.env.VERA_USER,
};

// Create sourcify_sync table to track verification attempts
const CREATE_SOURCIFY_SYNC_TABLE = `
  CREATE TABLE IF NOT EXISTS ${VERA_SCHEMA}.sourcify_sync (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    address BYTEA NOT NULL,
    verification_id UUID,
    status VARCHAR(50) NOT NULL, -- 'pending', 'submitted', 'verified', 'failed'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chain_id, address)
  );
`;

async function ensureTableExists(client) {
  await client.query(CREATE_SOURCIFY_SYNC_TABLE);
}

async function buildStandardJsonInput(verifiedContract, veraClient) {
  // Get the compilation data
  const {
    rows: [compilation],
  } = await veraClient.query(
    `SELECT * FROM ${VERA_SCHEMA}.compiled_contracts WHERE id = $1`,
    [verifiedContract.compilation_id]
  );

  // Get sources for this compilation
  const { rows: sources } = await veraClient.query(
    `
    SELECT s.content, ccs.path 
    FROM ${VERA_SCHEMA}.compiled_contracts_sources ccs
    JOIN ${VERA_SCHEMA}.sources s ON ccs.source_hash = s.source_hash
    WHERE ccs.compilation_id = $1
  `,
    [verifiedContract.compilation_id]
  );

  // Build sources object
  const sourcesObj = {};
  sources.forEach((source) => {
    sourcesObj[source.path] = {
      content: source.content,
    };
  });

  // Parse compiler settings
  let settings = {};
  try {
    settings =
      typeof compilation.compiler_settings === "string"
        ? JSON.parse(compilation.compiler_settings)
        : compilation.compiler_settings || {};
  } catch (error) {
    console.warn(
      `Failed to parse compiler_settings for compilation ${compilation.id}:`,
      error
    );
  }

  // Build standard JSON input
  const standardJsonInput = {
    language: compilation.language === "solidity" ? "Solidity" : "Vyper",
    sources: sourcesObj,
    settings: {
      ...settings,
      outputSelection: {
        "*": {
          "*": ["*"],
        },
      },
    },
  };

  return {
    stdJsonInput: standardJsonInput,
    compilerVersion: compilation.version,
    contractIdentifier: compilation.fully_qualified_name || compilation.name,
  };
}

async function submitToSourcify(chainId, address, verificationData) {
  const url = `${SOURCIFY_API_BASE}/v2/verify/${chainId}/${address}`;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(verificationData),
    });

    const responseData = await response.json();

    if (response.status === 202) {
      // Verification job started
      return {
        success: true,
        verificationId: responseData.verificationId,
        status: "submitted",
        message: "Verification job started successfully",
      };
    } else if (response.status === 409) {
      // Already verified
      return {
        success: true,
        status: "verified",
        message: "Contract already verified",
      };
    } else {
      return {
        success: false,
        status: "failed",
        error: responseData.error || `HTTP ${response.status}`,
      };
    }
  } catch (error) {
    console.error(
      `Error submitting to Sourcify for ${chainId} ${address}:`,
      error
    );
    return {
      success: false,
      status: "failed",
      error: error.message,
    };
  }
}

async function checkVerificationStatus(verificationId) {
  const url = `${SOURCIFY_API_BASE}/v2/verify/${verificationId}`;

  try {
    const response = await fetch(url);
    const data = await response.json();

    if (response.ok && data.isJobCompleted && data.contract.match != null) {
      return {
        success: true,
        status: data.status || "pending",
        data,
      };
    } else {
      return {
        success: false,
        error: data.error || `HTTP ${response.status}`,
      };
    }
  } catch (error) {
    return {
      success: false,
      error: error.message,
    };
  }
}

// We call processContract for each verified contract, even contracts that were already sent to Sourcify marked as 'submitted'.
// If the contract is marked as 'submitted' we check the status of the verification.
//    If the verification is completed, we update the status to 'verified'.
//    If the verification is already verified, we update the status to 'already_verified'.
//    If the verfication is failed, we update the status to 'failed'
// If the contract is not submitted, we build the standard JSON input and submit it to Sourcify.
//    If the submission is successful, we update the status to 'submitted' and store the verification
async function processContract(verifiedContract, veraPool) {
  let veraClient;

  try {
    veraClient = await veraPool.connect();

    // Get deployment info
    const {
      rows: [deployment],
    } = await veraClient.query(
      `SELECT * FROM ${VERA_SCHEMA}.contract_deployments WHERE id = $1`,
      [verifiedContract.deployment_id]
    );

    const chainId = deployment.chain_id;
    const address = deployment.address.toString("hex");

    if (verifiedContract.status === "verified") {
      console.log(`Already verified: [${chainId}] 0x${address}`);
      return;
    } else if (
      verifiedContract.status === "submitted" &&
      verifiedContract.verification_id
    ) {
      // Check status of pending verification
      const statusResult = await checkVerificationStatus(
        verifiedContract.verification_id
      );
      if (statusResult.success && statusResult.status === "completed") {
        await veraClient.query(
          `UPDATE ${VERA_SCHEMA}.sourcify_sync SET status = 'verified', updated_at = CURRENT_TIMESTAMP WHERE id = $1`,
          [verifiedContract.sync_id]
        );
        console.log(`Verification completed: [${chainId}] 0x${address}`);
        return;
      } else if (
        !statusResult.success &&
        statusResult.error.customCode === "already_verified"
      ) {
        await veraClient.query(
          `UPDATE ${VERA_SCHEMA}.sourcify_sync SET status = 'already_verified', updated_at = CURRENT_TIMESTAMP WHERE id = $1`,
          [verifiedContract.sync_id]
        );
        console.log(
          `Verification is already verified: [${chainId}] 0x${address}`
        );
        return;
      } else if (!statusResult.success) {
        console.error(
          `Error checking verification status for [${chainId}] 0x${address}:`,
          statusResult.error
        );
        await veraClient.query(
          `UPDATE ${VERA_SCHEMA}.sourcify_sync SET status = 'failed', error_message = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
          [JSON.stringify(statusResult.error), verifiedContract.sync_id]
        );
        return;
      }
    }

    // Build verification data
    const verificationData = await buildStandardJsonInput(
      verifiedContract,
      veraClient
    );

    // Submit to Sourcify
    const result = await submitToSourcify(
      chainId,
      `0x${address}`,
      verificationData
    );

    // Update or insert sync record
    if (verifiedContract.verification_id) {
      await veraClient.query(
        `UPDATE ${VERA_SCHEMA}.sourcify_sync 
         SET verification_id = $1, status = $2, error_message = $3, updated_at = CURRENT_TIMESTAMP 
         WHERE chain_id = $4 AND address = $5`,
        [
          result.verificationId || null,
          result.status,
          result.error || null,
          chainId,
          deployment.address,
        ]
      );
    } else {
      await veraClient.query(
        `INSERT INTO ${VERA_SCHEMA}.sourcify_sync (chain_id, address, verification_id, status, error_message)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          chainId,
          deployment.address,
          result.verificationId || null,
          result.status,
          result.error || null,
        ]
      );
    }

    if (result.success) {
      console.log(
        `Submitted to Sourcify: [${chainId}] 0x${address} - ${
          result.status
        } - ${result.message || ""}`
      );
    } else {
      console.error(
        `Failed to submit: [${chainId}] 0x${address} - ${result.error}`
      );
    }
  } catch (error) {
    console.error(
      `Error processing contract ID ${verifiedContract.id}:`,
      error
    );
  } finally {
    if (veraClient) veraClient.release();
  }
}

(async () => {
  const connector = new Connector();
  const veraPoolOpts = await connector.getOptions({
    instanceConnectionName: process.env.VERA_INSTANCE_CONNECTION_NAME,
    authType: AuthTypes.PASSWORD,
  });
  const veraPool = new pg.Pool({
    ...veraPoolOpts,
    ...VERA_DB_CONFIG,
    password: process.env.VERA_PASSWORD,
  });

  try {
    // Ensure sourcify_sync table exists
    const client = await veraPool.connect();
    await ensureTableExists(client);
    client.release();

    let verifiedContractCount = 1;
    while (verifiedContractCount > 0) {
      const startIterationTime = performance.now();

      console.log(`Processing next ${N} contracts`);
      console.log(`Current contract id: ${CURRENT_VERIFIED_CONTRACT}`);

      // Fetch verified contracts from Vera that haven't been processed yet
      const { rows: verifiedContracts, rowCount } = await veraPool.query(
        `
        SELECT vc.* , ss.status, ss.verification_id, ss.id AS sync_id
        FROM ${VERA_SCHEMA}.verified_contracts vc
        JOIN ${VERA_SCHEMA}.contract_deployments cd ON vc.deployment_id = cd.id
        LEFT JOIN ${VERA_SCHEMA}.sourcify_sync ss ON cd.chain_id = ss.chain_id AND cd.address = ss.address
        WHERE vc.id >= $1
          AND (ss.id IS NULL OR ss.status IN ('pending', 'submitted'))
          AND cd.transaction_hash IS NOT NULL
          AND vc.created_by = 'routescan'
        ORDER BY vc.id ASC
        LIMIT $2
      `,
        [CURRENT_VERIFIED_CONTRACT, N]
      );

      verifiedContractCount = rowCount;

      // Process the batch sequentially to avoid overwhelming the API
      try {
        for (const contract of verifiedContracts) {
          await processContract(contract, veraPool);
          // Small delay between requests to be respectful to the API
          await new Promise((resolve) => setTimeout(resolve, 100));
        }

        // Update the counter file only after the batch successfully completes
        if (verifiedContracts.length > 0) {
          const lastProcessedId =
            verifiedContracts[verifiedContracts.length - 1].id;
          CURRENT_VERIFIED_CONTRACT = parseInt(lastProcessedId) + 1;
          fs.writeFile(
            COUNTER_FILE,
            CURRENT_VERIFIED_CONTRACT.toString(),
            "utf8",
            (err) => {
              if (err) {
                console.error("Error writing counter file:", err);
              }
            }
          );
        }
      } catch (batchError) {
        console.error("Error processing batch:", batchError);
      }

      const endIterationTime = performance.now();
      const iterationTimeTaken = endIterationTime - startIterationTime;
      console.log(
        `Rate: processing ${
          N / (iterationTimeTaken / 1000)
        } contracts per second`
      );
    }
    console.log("Contracts submitted to Sourcify successfully.");
  } catch (error) {
    console.error("Error submitting contracts to Sourcify:", error);
  } finally {
    if (veraPool) await veraPool.end();
  }
})();
