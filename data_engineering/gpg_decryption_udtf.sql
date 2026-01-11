/*
Script: gpg_decryption_udtf.sql
Description: DDL statements to create the GPG decryption UDTF.
Team: Data Engineering
Date: 2026-01-10
Parameters: csch - common Schema identifier (e.g., COMMON) | tsch - Target Schema identifier (e.g., DE))
*/
CREATE FUNCTION IF NOT EXISTS HACKATHON_DB.HACKATHON_{{tsch}}_SCH.GPG_DECRYPTION_UDTF(file_path string)
returns table(line string)
language python
runtime_version = '3.11'
external_access_integrations=(GENERAL_EAI)
secrets = ('pvt_key'=HACKATHON_DB.HACKATHON_{{csch}}_SCH.PGP_PRIVATE_KEY
           ,'pphrase'=HACKATHON_DB.HACKATHON_{{csch}}_SCH.PGP_PASSPHRASE)
packages=('snowflake-snowpark-python', 'python-gnupg')
handler='gpgdecryptor'
log_level=info
comment = 'Created by Prakash Loganathan'
as
$$
import _snowflake
import gnupg
import io
import os
import logging
from snowflake.snowpark.files import SnowflakeFile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('GPG Decryption')

class gpgdecryptor:
    def __init__(self):
        private_key = _snowflake.get_generic_secret_string('pvt_key')
        os.makedirs('/tmp/gnupg', exist_ok=True)
        self.gpg = gnupg.GPG(gnupghome='/tmp/gnupg')
        import_result = self.gpg.import_keys(private_key)
        if not import_result.count:
            raise ValueError("Failed to import GPG Private Kay")
        logger.info("GPG Private Key Imported Successfully")

    def process(self, file_path: str):
        passphrase = _snowflake.get_generic_secret_string('pphrase')
        logger.info(f"Decryption process starts...")

        with SnowflakeFile.open(file_path, 'rb') as encrypted_file:
            decrypted_data = self.gpg.decrypt_file(encrypted_file, passphrase=passphrase)

            if not decrypted_data.ok:
                logger.error(f"Decryption Failed: {decrypted_data.stderr}")
                raise ValueError(f"Decryption Failed: {decrypted_data.stderr}")

            decrypted_content = decrypted_data.data.decode('utf-8')
            logger.info('Decryption Successfull. Yielding lines')
            for line in io.StringIO(decrypted_content):
                yield(line.strip(),)
$$
;
