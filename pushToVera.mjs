import pg from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

dotenv.config();

console.log("Starting Push to Verifier Alliance DB");

const __filename = fileURLToPath(import.meta.url); // get the resolved path to the file
const __dirname = path.dirname(__filename);

const { Client } = pg;

// Load current verified contract counter from file
const COUNTER_FILE = path.join(__dirname, "CURRENT_VERIFIED_CONTRACT");
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
  host: process.env.VERA_HOST,
  database: process.env.VERA_DB,
  user: process.env.VERA_USER,
  password: process.env.VERA_PASSWORD,
  port: process.env.VERA_PORT,
};

const sourceClient = new Client(SOURCE_DB_CONFIG);
const targetClient = new Client(TARGET_DB_CONFIG);

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

(async () => {
  try {
    await sourceClient.connect();
    await targetClient.connect();

    let verifiedContractCount = 1;

    while (verifiedContractCount > 0) {
      const startIterationTime = performance.now();

      console.log(`Processing next ${N} contracts`);
      console.log(`Current contract id: ${CURRENT_VERIFIED_CONTRACT}`);

      const { rows: countLeft } = await sourceClient.query(
        `
          SELECT count(vc.*) 
          FROM ${SOURCIFY_SCHEMA}.sourcify_matches sm
          JOIN ${SOURCIFY_SCHEMA}.verified_contracts vc ON vc.id = sm.verified_contract_id
          JOIN ${SOURCIFY_SCHEMA}.contract_deployments cd on vc.deployment_id = cd.id 
          JOIN ${SOURCIFY_SCHEMA}.contracts c on cd.contract_id = c.id 
          JOIN ${SOURCIFY_SCHEMA}.code on code.code_hash = c.creation_code_hash 
          WHERE 1=1
          and sm.creation_match is not null
          and sm.runtime_match is not null
          and cd.transaction_hash is not null
          and code.code is not null
          and vc.id > $1;
        `,
        [CURRENT_VERIFIED_CONTRACT]
      );
      console.log("Number of contracts left: ", countLeft[0].count);

      const { rows: verifiedContracts, rowCount } = await sourceClient.query(
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
            and vc.id > $1
          ORDER BY vc.id ASC
          LIMIT $2;
        `,
        [CURRENT_VERIFIED_CONTRACT, N]
      );

      verifiedContractCount = rowCount;

      for (const contract of verifiedContracts) {
        const {
          id: verifiedContractId,
          deployment_id,
          compilation_id,
        } = contract;

        // Update the CURRENT_VERIFIED_CONTRACT counter
        CURRENT_VERIFIED_CONTRACT = verifiedContractId;
        fs.writeFileSync(
          COUNTER_FILE,
          CURRENT_VERIFIED_CONTRACT.toString(),
          "utf8"
        );

        // Get all information for verified_contracts
        const verifiedContract = contract;

        // Get all FK information
        const {
          rows: [deployment],
        } = await sourceClient.query(
          `SELECT * FROM ${SOURCIFY_SCHEMA}.contract_deployments WHERE id = $1`,
          [deployment_id]
        );
        const {
          rows: [deploymentContract],
        } = await sourceClient.query(
          `SELECT * FROM ${SOURCIFY_SCHEMA}.contracts WHERE id = $1`,
          [deployment.contract_id]
        );
        const {
          rows: [compilation],
        } = await sourceClient.query(
          `SELECT * FROM ${SOURCIFY_SCHEMA}.compiled_contracts WHERE id = $1`,
          [compilation_id]
        );
        if (compilation.creation_code_hash === null) {
          continue;
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

        // Get sources for the compilation
        const { rows: compilationSources } = await sourceClient.query(
          `SELECT * FROM ${SOURCIFY_SCHEMA}.compiled_contracts_sources WHERE compilation_id = $1`,
          [compilation_id]
        );

        // Extract all source_hashes
        const sourceHashes = compilationSources.map(
          (source) => source.source_hash
        );

        // Fetch all relevant sources in a single query
        const { rows: sourcesContent } = await sourceClient.query(
          `SELECT * FROM ${SOURCIFY_SCHEMA}.sources WHERE source_hash = ANY($1)`,
          [sourceHashes]
        );

        // Create a map for quick lookup
        const sourceContentMap = new Map(
          sourcesContent.map((source) => [
            source.source_hash.toString("hex"),
            source,
          ])
        );

        // Insert sources and compiled_contracts_sources
        for (const source of compilationSources) {
          const sourceContent = sourceContentMap.get(
            source.source_hash.toString("hex")
          );

          if (!sourceContent) {
            console.warn(
              `Source content not found for hash: ${source.source_hash.toString(
                "hex"
              )}`
            );
            continue;
          }

          // Insert into sources table
          const sourceValues = [
            Buffer.from(sourceContent.source_hash),
            Buffer.from(sourceContent.source_hash_keccak),
            sourceContent.content,
          ];
          await targetClient.query(
            `
            INSERT INTO ${VERA_SCHEMA}.sources (source_hash, source_hash_keccak, content)
            VALUES ($1, $2, $3)
            ON CONFLICT (source_hash) DO NOTHING
            `,
            sourceValues
          );

          // Insert into compiled_contracts_sources table
          const compiledSourcesValues = [
            newCompilationId,
            Buffer.from(source.source_hash),
            source.path,
          ];
          await targetClient.query(
            `
            INSERT INTO ${VERA_SCHEMA}.compiled_contracts_sources (compilation_id, source_hash, path)
            VALUES ($1, $2, $3)
            ON CONFLICT (compilation_id, path) DO NOTHING
            `,
            compiledSourcesValues
          );
        }

        console.log({
          "verifiedContract.created_at": verifiedContract.created_at,
          "verifiedContract.updated_at": verifiedContract.updated_at,
          "verifiedContract.created_by": verifiedContract.created_by,
          "verifiedContract.updated_by": verifiedContract.updated_by,
          newDeploymentId: newDeploymentId,
          newCompilationId: newCompilationId,
          "verifiedContract.creation_match": verifiedContract.creation_match,
          "verifiedContract.creation_values": verifiedContract.creation_values,
          "JSON.stringify(verifiedContract.creation_transformations)":
            JSON.stringify(verifiedContract.creation_transformations),
          "verifiedContract.runtime_match": verifiedContract.runtime_match,
          "verifiedContract.runtime_values": verifiedContract.runtime_values,
          "JSON.stringify(verifiedContract.runtime_transformations":
            JSON.stringify(verifiedContract.runtime_transformations),
        });
        await targetClient.query(
          `
            INSERT INTO ${VERA_SCHEMA}.verified_contracts (
                created_at, updated_at, created_by, updated_by, deployment_id, compilation_id, 
                creation_match, creation_values, creation_transformations, creation_metadata_match,
                runtime_match, runtime_values, runtime_transformations, runtime_metadata_match)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (compilation_id, deployment_id) DO NOTHING
          `,
          [
            verifiedContract.created_at,
            verifiedContract.updated_at,
            verifiedContract.created_by,
            verifiedContract.updated_by,
            newDeploymentId,
            newCompilationId,
            verifiedContract.creation_match,
            verifiedContract.creation_values,
            JSON.stringify(verifiedContract.creation_transformations),
            verifiedContract.creation_metadata_match,
            verifiedContract.runtime_match,
            verifiedContract.runtime_values,
            JSON.stringify(verifiedContract.runtime_transformations),
            verifiedContract.runtime_metadata_match,
          ]
        );
      }

      const endIterationTime = performance.now();
      const iterationTimeTaken = endIterationTime - startIterationTime;
      console.log(
        `Rate: processing ${
          N / (iterationTimeTaken / 1000)
        } contracts per second`
      );
      console.log();
    }
    console.log("Contracts transferred successfully.");
  } catch (error) {
    console.error("Error transferring contracts:", error);
  } finally {
    await sourceClient.end();
    await targetClient.end();
  }
})();
