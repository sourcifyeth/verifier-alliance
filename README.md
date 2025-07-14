# Sourcify -> Verifier Alliance sync

This script is needed to push all verified_contracts from one Sourcify to VerA database. VerA is a live database in which other entities are writing while the script is running.

## Configure

1. Copy and paste `.env.template` to `.env` and fill it appropriately.

## Run

`npm run vera:push`

## How it works

0. CURRENT_VERIFIED_CONTRACT is a variable stored in a permanent file, default 1.
1. Extract N verified_contracts starting from CURRENT_VERIFIED_CONTRACT
2. For each verified_contract:
   a. Update the CURRENT_VERIFIED_CONTRACT counter
   b. Get all information from verified_contracts
   c. Get all the FK information
   d. Insert all the verified_contracts dependencies (compiled_contracts,contract_deployments,contracts,code) and verified_contracts using the new ids as FKs

# Verifier Alliance -> Sourcify sync (historical data)

This script fetches verified contracts from Verifier Alliance database and submits them to Sourcify's verification API.

## Configure

1. Copy and paste `.env.template` to `.env` and fill it appropriately.

## Run

`npm run sourcify:push`

## How it works

0. CURRENT_SOURCIFY_SYNC is a variable stored in a permanent file, default 1.
1. Extract N verified_contracts starting from CURRENT_SOURCIFY_SYNC that haven't been processed yet
2. For each verified_contract:
   a. Build standard JSON input from compilation data
   b. Submit to Sourcify API using /v2/verify/{chainId}/{address}
   c. Track submission status in sourcify_sync table
   d. Check pending verifications using /v2/verify/{verificationId}
   e. Update the CURRENT_SOURCIFY_SYNC counter

The script creates a sourcify_sync table to track verification attempts and prevent re-processing.

# Verifier Alliance -> Sourcify sync (real time data)

This daemon listens for `new_verified_contract` PostgreSQL notification and sends requests to sourcify-server.

## Configuration

1. Run the following SQL queries on the Verifier Alliance database to set up the notification.

   ```sql
   CREATE OR REPLACE FUNCTION notify_new_verified_contract()
   RETURNS TRIGGER AS $$
   BEGIN
      PERFORM pg_notify(
         'new_verified_contract',
         json_build_object(
            'id', NEW.id,
            'created_by', NEW.created_by
         )::text
      );
      RETURN NEW;
   END;
   $$ LANGUAGE plpgsql;


   CREATE TRIGGER new_verified_contract_trigger
   AFTER INSERT ON verified_contracts
   FOR EACH ROW
   EXECUTE FUNCTION notify_new_verified_contract();
   ```

2. Copy-paste `.env.template` to `.env` and fill it appropriately.

## Run

```bash
npm run vera:pull
```

or with docker:

```bash
docker build  -t sourcify-verifier-alliance .
```

```bash
docker run --name sourcify-verifier-alliance --env-file=.env  sourcify-verifier-alliance
```
