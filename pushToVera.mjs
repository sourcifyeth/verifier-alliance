import pg from "pg";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import { AuthTypes, Connector } from "@google-cloud/cloud-sql-connector";

dotenv.config();

console.log("Starting Push to Verifier Alliance DB");

const CURRENT_VERIFIED_CONTRACT_PATH =
  process.env.CURRENT_VERIFIED_CONTRACT_PATH;

if (!CURRENT_VERIFIED_CONTRACT_PATH) {
  throw new Error("CURRENT_VERIFIED_CONTRACT_PATH is not set");
}

const { Pool } = pg;

// Load current verified contract counter from file
const COUNTER_FILE = path.join(
  CURRENT_VERIFIED_CONTRACT_PATH,
  "CURRENT_VERIFIED_CONTRACT"
);
let CURRENT_VERIFIED_CONTRACT = 1;
if (fs.existsSync(COUNTER_FILE)) {
  CURRENT_VERIFIED_CONTRACT = parseInt(
    fs.readFileSync(COUNTER_FILE, "utf8"),
    10
  );
}

const N = 200; // Number of contracts to process at a time

const SOURCIFY_SCHEMA = process.env.SOURCIFY_SCHEMA || "public";
const VERA_SCHEMA = process.env.VERA_SCHEMA || "public";

const SOURCE_DB_CONFIG = {
  host: process.env.SOURCIFY_HOST,
  database: process.env.SOURCIFY_DB,
  user: process.env.SOURCIFY_USER,
  password: process.env.SOURCIFY_PASSWORD,
  port: process.env.SOURCIFY_PORT,
};

const TARGET_DB_CONFIG = {
  database: process.env.VERA_DB,
  user: process.env.VERA_USER,
};

async function upsertAndGetId(
  query,
  conflictQuery,
  insertValues,
  selectValues,
  client
) {
  await client.query(query, insertValues);
  const { rows } = await client.query(conflictQuery, selectValues);
  return rows[0].id;
}

async function processContract(
  contract,
  sourcePool,
  targetPool,
  SOURCIFY_SCHEMA,
  VERA_SCHEMA
) {
  let sourceClient;
  let targetClient;

  try {
    sourceClient = await sourcePool.connect();
    targetClient = await targetPool.connect();

    await targetClient.query("BEGIN");

    const { id: verifiedContractId } = contract;

    const {
      rows: [combinedData],
    } = await sourceClient.query(
      `
      SELECT
        cd.id as deployment_id, cd.chain_id, cd.address, cd.transaction_hash, cd.block_number, cd.transaction_index, cd.deployer, cd.contract_id,
        c.id as contract_id_alias, c.creation_code_hash as contract_creation_code_hash, c.runtime_code_hash as contract_runtime_code_hash,
        cc.id as compilation_id_alias, cc.compiler, cc.language, cc.creation_code_hash as compilation_creation_code_hash, cc.runtime_code_hash as compilation_runtime_code_hash, cc.version, cc.name, cc.fully_qualified_name, cc.compiler_settings, cc.compilation_artifacts, cc.creation_code_artifacts, cc.runtime_code_artifacts
      FROM ${SOURCIFY_SCHEMA}.verified_contracts vc
      JOIN ${SOURCIFY_SCHEMA}.contract_deployments cd ON vc.deployment_id = cd.id
      JOIN ${SOURCIFY_SCHEMA}.contracts c ON cd.contract_id = c.id
      JOIN ${SOURCIFY_SCHEMA}.compiled_contracts cc ON vc.compilation_id = cc.id
      WHERE vc.id = $1
      `,
      [verifiedContractId]
    );

    if (!combinedData) {
      console.warn(
        `No combined data found for verified contract ID ${verifiedContractId}`
      );
      await targetClient.query("ROLLBACK");
      return;
    }

    // Extract data for deployment, deploymentContract, and compilation
    const deployment = {
      id: combinedData.deployment_id,
      chain_id: combinedData.chain_id,
      address: combinedData.address,
      transaction_hash: combinedData.transaction_hash,
      block_number: combinedData.block_number,
      transaction_index: combinedData.transaction_index,
      deployer: combinedData.deployer,
      contract_id: combinedData.contract_id,
    };

    const deploymentContract = {
      id: combinedData.contract_id_alias,
      creation_code_hash: combinedData.contract_creation_code_hash,
      runtime_code_hash: combinedData.contract_runtime_code_hash,
    };

    const compilation = {
      id: combinedData.compilation_id_alias,
      compiler: combinedData.compiler,
      language: combinedData.language,
      creation_code_hash: combinedData.compilation_creation_code_hash,
      runtime_code_hash: combinedData.compilation_runtime_code_hash,
      version: combinedData.version,
      name: combinedData.name,
      fully_qualified_name: combinedData.fully_qualified_name,
      compiler_settings: combinedData.compiler_settings,
      compilation_artifacts: combinedData.compilation_artifacts,
      creation_code_artifacts: combinedData.creation_code_artifacts,
      runtime_code_artifacts: combinedData.runtime_code_artifacts,
    };

    // Check for null creation_code_hash *after* fetching the data
    if (compilation.creation_code_hash === null) {
      await targetClient.query("ROLLBACK");
      console.log(
        `Skipping contract due to null creation_code_hash: ${verifiedContractId}`
      );
      return;
    }

    // Get creation and runtime code for the compilation
    const {
      rows: [compilationCreationCode],
    } = await sourceClient.query(
      `SELECT * FROM ${SOURCIFY_SCHEMA}.code WHERE code_hash = $1`,
      [compilation.creation_code_hash]
    );
    const {
      rows: [compilationRuntimeCode],
    } = await sourceClient.query(
      `SELECT * FROM ${SOURCIFY_SCHEMA}.code WHERE code_hash = $1`,
      [compilation.runtime_code_hash]
    );

    // Get creation and runtime code for the deployment
    const {
      rows: [deployedCreationCode],
    } = await sourceClient.query(
      `SELECT * FROM ${SOURCIFY_SCHEMA}.code WHERE code_hash = $1`,
      [deploymentContract.creation_code_hash]
    );
    const {
      rows: [deployedRuntimeCode],
    } = await sourceClient.query(
      `SELECT * FROM ${SOURCIFY_SCHEMA}.code WHERE code_hash = $1`,
      [deploymentContract.runtime_code_hash]
    );

    // Insert dependencies into target DB and handle conflicts

    // Insert creation and runtime code for the compilation into target DB and handle conflicts
    await targetClient.query(
      `
      INSERT INTO ${VERA_SCHEMA}.code (code_hash, code, code_hash_keccak)
      VALUES ($1, $2, $3)
      ON CONFLICT (code_hash) DO NOTHING
    `,
      [
        compilationCreationCode.code_hash,
        compilationCreationCode.code,
        compilationCreationCode.code_hash_keccak,
      ]
    );

    await targetClient.query(
      `
      INSERT INTO ${VERA_SCHEMA}.code (code_hash, code, code_hash_keccak)
      VALUES ($1, $2, $3)
      ON CONFLICT (code_hash) DO NOTHING
    `,
      [
        compilationRuntimeCode.code_hash,
        compilationRuntimeCode.code,
        compilationRuntimeCode.code_hash_keccak,
      ]
    );

    // Insert creation and runtime code for the deployment into target DB and handle conflicts
    await targetClient.query(
      `
      INSERT INTO ${VERA_SCHEMA}.code (code_hash, code, code_hash_keccak)
      VALUES ($1, $2, $3)
      ON CONFLICT (code_hash) DO NOTHING
    `,
      [
        deployedCreationCode.code_hash,
        deployedCreationCode.code,
        deployedCreationCode.code_hash_keccak,
      ]
    );

    await targetClient.query(
      `
      INSERT INTO ${VERA_SCHEMA}.code (code_hash, code, code_hash_keccak)
      VALUES ($1, $2, $3)
      ON CONFLICT (code_hash) DO NOTHING
    `,
      [
        deployedRuntimeCode.code_hash,
        deployedRuntimeCode.code,
        deployedRuntimeCode.code_hash_keccak,
      ]
    );

    const newContractIdValues = [
      deploymentContract.creation_code_hash,
      deploymentContract.runtime_code_hash,
    ];
    const newContractId = await upsertAndGetId(
      `
      INSERT INTO ${VERA_SCHEMA}.contracts (creation_code_hash, runtime_code_hash)
      VALUES ($1, $2)
      ON CONFLICT (creation_code_hash, runtime_code_hash) DO NOTHING
    `,
      `
      SELECT id FROM ${VERA_SCHEMA}.contracts WHERE creation_code_hash = $1 AND runtime_code_hash = $2
    `,
      newContractIdValues,
      newContractIdValues,
      targetClient
    );

    const newDeploymentIdValues = [
      deployment.chain_id,
      deployment.address,
      deployment.transaction_hash,
      deployment.block_number,
      deployment.transaction_index,
      deployment.deployer,
      newContractId,
    ];
    const newDeploymentId = await upsertAndGetId(
      `
      INSERT INTO ${VERA_SCHEMA}.contract_deployments (chain_id, address, transaction_hash, block_number, transaction_index, deployer, contract_id)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (chain_id, address, transaction_hash) DO NOTHING
    `,
      `
      SELECT id FROM ${VERA_SCHEMA}.contract_deployments WHERE chain_id = $1 AND address = $2 AND transaction_hash = $3
    `,
      newDeploymentIdValues,
      newDeploymentIdValues.slice(0, 3),
      targetClient
    );

    const newCompilationIdValues = [
      compilation.compiler,
      compilation.language,
      compilation.creation_code_hash,
      compilation.runtime_code_hash,
      compilation.version,
      compilation.name,
      compilation.fully_qualified_name,
      compilation.compiler_settings,
      compilation.compilation_artifacts,
      compilation.creation_code_artifacts,
      compilation.runtime_code_artifacts,
    ];
    const newCompilationId = await upsertAndGetId(
      `
      INSERT INTO ${VERA_SCHEMA}.compiled_contracts (
          compiler, language, creation_code_hash, runtime_code_hash, version, name, fully_qualified_name,
          compiler_settings, compilation_artifacts, creation_code_artifacts, runtime_code_artifacts)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      ON CONFLICT (compiler, language, creation_code_hash, runtime_code_hash) DO NOTHING
    `,
      `
      SELECT id FROM ${VERA_SCHEMA}.compiled_contracts
      WHERE compiler = $1 AND language = $2 AND creation_code_hash = $3 AND runtime_code_hash = $4
    `,
      newCompilationIdValues,
      newCompilationIdValues.slice(0, 4),
      targetClient
    );

    const { rows: allSourcesData } = await sourceClient.query(
      `
      SELECT
          ccs.source_hash,
          ccs.path,
          s.source_hash_keccak,
          s.content
      FROM ${SOURCIFY_SCHEMA}.compiled_contracts_sources ccs
      JOIN ${SOURCIFY_SCHEMA}.sources s ON ccs.source_hash = s.source_hash
      WHERE ccs.compilation_id = $1
      `,
      [compilation.id]
    );

    if (allSourcesData.length > 0) {
      const uniqueSources = new Map();
      allSourcesData.forEach((row) => {
        // Use hex string as key for Map uniqueness based on buffer content
        const hexHash = Buffer.from(row.source_hash).toString("hex");
        if (!uniqueSources.has(hexHash)) {
          uniqueSources.set(hexHash, {
            source_hash: Buffer.from(row.source_hash),
            source_hash_keccak: Buffer.from(row.source_hash_keccak),
            content: row.content,
          });
        }
      });

      const sourceValuesParams = [];
      const sourceValuesTuples = [];
      let sourceParamIndex = 1;
      uniqueSources.forEach((source) => {
        // Construct tuples like ($1, $2, $3), ($4, $5, $6), ...
        sourceValuesTuples.push(
          `($${sourceParamIndex++}, $${sourceParamIndex++}, $${sourceParamIndex++})`
        );
        // Flatten parameters into a single array
        sourceValuesParams.push(
          source.source_hash,
          source.source_hash_keccak,
          source.content
        );
      });

      // Only execute if there are sources to insert
      if (sourceValuesParams.length > 0) {
        await targetClient.query(
          `
            INSERT INTO ${VERA_SCHEMA}.sources (source_hash, source_hash_keccak, content)
            VALUES ${sourceValuesTuples.join(", ")}
            ON CONFLICT (source_hash) DO NOTHING
            `,
          sourceValuesParams
        );
      }

      const compiledSourcesValuesParams = [];
      const compiledSourcesValuesTuples = [];
      let compiledSourcesParamIndex = 1;
      allSourcesData.forEach((row) => {
        // Construct tuples like ($1, $2, $3), ($4, $5, $6), ...
        compiledSourcesValuesTuples.push(
          `($${compiledSourcesParamIndex++}, $${compiledSourcesParamIndex++}, $${compiledSourcesParamIndex++})`
        );
        // Flatten parameters into a single array
        compiledSourcesValuesParams.push(
          newCompilationId,
          Buffer.from(row.source_hash),
          row.path
        );
      });

      // Only execute if there are compiled sources links to insert
      if (compiledSourcesValuesParams.length > 0) {
        await targetClient.query(
          `
            INSERT INTO ${VERA_SCHEMA}.compiled_contracts_sources (compilation_id, source_hash, path)
            VALUES ${compiledSourcesValuesTuples.join(", ")}
            ON CONFLICT (compilation_id, path) DO NOTHING
            `,
          compiledSourcesValuesParams
        );
      }
    }

    const result = await targetClient.query(
      `
      INSERT INTO ${VERA_SCHEMA}.verified_contracts (
          created_at, updated_at, created_by, updated_by, deployment_id, compilation_id,
          creation_match, creation_values, creation_transformations, creation_metadata_match,
          runtime_match, runtime_values, runtime_transformations, runtime_metadata_match)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      ON CONFLICT (compilation_id, deployment_id) DO NOTHING RETURNING id
    `,
      [
        contract.created_at,
        contract.updated_at,
        contract.created_by,
        contract.updated_by,
        newDeploymentId,
        newCompilationId,
        contract.creation_match,
        contract.creation_values,
        JSON.stringify(contract.creation_transformations),
        contract.creation_metadata_match,
        contract.runtime_match,
        contract.runtime_values,
        JSON.stringify(contract.runtime_transformations),
        contract.runtime_metadata_match,
      ]
    );
    if (result.rows.length !== 0) {
      await targetClient.query("COMMIT");
      console.log(
        `Pushed: [${deployment.chain_id.toString()}] 0x${deployment.address.toString(
          "hex"
        )} (old ID: ${verifiedContractId})`
      );
    } else {
      await targetClient.query("ROLLBACK");
      console.log(
        `Already pushed: [${deployment.chain_id.toString()}] 0x${deployment.address.toString(
          "hex"
        )} (old ID: ${verifiedContractId})`
      );
    }
  } catch (error) {
    await targetClient.query("ROLLBACK");
    console.error(`Error processing contract ID ${contract.id}:`, error);
  } finally {
    // Release connections if they were acquired
    if (sourceClient) sourceClient.release();
    if (targetClient) targetClient.release();
  }
}

(async () => {
  // Connect to source DB using a Pool
  const sourcePool = new Pool(SOURCE_DB_CONFIG);
  sourcePool.on("error", (err, client) => {
    console.error("Unexpected error on idle source client", err);
    process.exit(-1);
  });

  const connector = new Connector();
  const targetPoolOpts = await connector.getOptions({
    instanceConnectionName: process.env.VERA_INSTANCE_CONNECTION_NAME,
    authType: AuthTypes.PASSWORD,
  });
  const targetPool = new pg.Pool({
    ...targetPoolOpts,
    ...TARGET_DB_CONFIG,
    password: process.env.VERA_PASSWORD,
  });

  try {
    // Process contracts
    let verifiedContractCount = 1;
    while (verifiedContractCount > 0) {
      const startIterationTime = performance.now();

      console.log(`Processing next ${N} contracts`);
      console.log(`Current contract id: ${CURRENT_VERIFIED_CONTRACT}`);

      const { rows: verifiedContracts, rowCount } = await sourcePool.query(
        `
          SELECT vc.* FROM ${SOURCIFY_SCHEMA}.sourcify_matches sm
          JOIN ${SOURCIFY_SCHEMA}.verified_contracts vc ON vc.id = sm.verified_contract_id
          JOIN ${SOURCIFY_SCHEMA}.contract_deployments cd on vc.deployment_id = cd.id 
          JOIN ${SOURCIFY_SCHEMA}.contracts c on cd.contract_id = c.id 
          JOIN ${SOURCIFY_SCHEMA}.code on code.code_hash = c.creation_code_hash 
          WHERE 1=1
            and sm.creation_match is not null
            and sm.runtime_match is not null
            and cd.transaction_hash is not null
            and code.code is not null
            and vc.id >= $1
          ORDER BY vc.id ASC
          LIMIT $2;
        `,
        [CURRENT_VERIFIED_CONTRACT, N]
      );

      verifiedContractCount = rowCount;

      // Process the batch in parallel
      try {
        const processingPromises = verifiedContracts.map((contract) =>
          processContract(
            contract,
            sourcePool,
            targetPool,
            SOURCIFY_SCHEMA,
            VERA_SCHEMA
          )
        );
        await Promise.all(processingPromises);

        // Update the counter file only after the batch successfully completes (or handles errors individually)
        if (verifiedContracts.length > 0) {
          const lastProcessedId =
            verifiedContracts[verifiedContracts.length - 1].id;
          CURRENT_VERIFIED_CONTRACT = parseInt(lastProcessedId) + 1; // Start next batch from the next ID
          // Use async write to avoid blocking
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
    console.log("Contracts transferred successfully.");
  } catch (error) {
    console.error("Error transferring contracts:", error);
  } finally {
    // End both pools
    if (sourcePool) await sourcePool.end();
    if (targetPool) await targetPool.end();
  }
})();
