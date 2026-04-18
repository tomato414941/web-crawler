ALTER TABLE url_ledger
    DROP COLUMN IF EXISTS lease_token,
    DROP COLUMN IF EXISTS lease_expires_at;
