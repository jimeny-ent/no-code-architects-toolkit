import os
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Retrieve the API key with enhanced validation
def get_api_key() -> str:
    api_key = os.environ.get('API_KEY')
    if not api_key:
        logger.error("API_KEY environment variable is not set")
        raise ValueError("API_KEY environment variable is mandatory")
    return api_key

# GCP environment variables with logging
def get_gcp_config() -> tuple:
    credentials = os.environ.get('GCP_SA_CREDENTIALS', '')
    bucket_name = os.environ.get('GCP_BUCKET_NAME', '')
    
    if not credentials or not bucket_name:
        logger.warning("Incomplete GCP configuration detected")
    
    return credentials, bucket_name

# S3 environment variables with logging
def get_s3_config() -> tuple:
    return (
        os.environ.get('S3_ENDPOINT_URL', ''),
        os.environ.get('S3_ACCESS_KEY', ''),
        os.environ.get('S3_SECRET_KEY', ''),
        os.environ.get('S3_BUCKET_NAME', ''),
        os.environ.get('S3_REGION', '')
    )

def validate_env_vars(provider: str) -> None:
    """Validate environment variables for storage providers"""
    required_vars = {
        'GCP': ['GCP_BUCKET_NAME', 'GCP_SA_CREDENTIALS'],
        'S3': ['S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_BUCKET_NAME']
    }
    
    missing_vars = [var for var in required_vars[provider] if not os.getenv(var)]
    
    if missing_vars:
        error_msg = f"Missing environment variables for {provider} storage: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

class CloudStorageProvider:
    """Abstract base class for cloud storage providers"""
    def upload_file(self, file_path: str) -> str:
        raise NotImplementedError("Subclasses must implement upload_file method")

class GCPStorageProvider(CloudStorageProvider):
    """GCP-specific cloud storage provider"""
    def __init__(self):
        credentials, bucket_name = get_gcp_config()
        self.bucket_name = bucket_name
        logger.info(f"Initialized GCP Storage Provider for bucket: {bucket_name}")

    def upload_file(self, file_path: str) -> str:
        from services.gcp_toolkit import upload_to_gcs
        try:
            return upload_to_gcs(file_path, self.bucket_name)
        except Exception as e:
            logger.error(f"GCP upload failed: {str(e)}")
            raise

class S3CompatibleProvider(CloudStorageProvider):
    """S3-compatible storage provider"""
    def __init__(self):
        endpoint, access_key, secret_key, bucket_name, region = get_s3_config()
        self.bucket_name = bucket_name
        self.region = region
        self.endpoint_url = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        logger.info(f"Initialized S3 Storage Provider for bucket: {bucket_name}")

    def upload_file(self, file_path: str) -> str:
        from services.s3_toolkit import upload_to_s3
        try:
            return upload_to_s3(
                file_path, 
                self.bucket_name, 
                self.region, 
                self.endpoint_url, 
                self.access_key, 
                self.secret_key
            )
        except Exception as e:
            logger.error(f"S3 upload failed: {str(e)}")
            raise

def get_storage_provider() -> CloudStorageProvider:
    """
    Dynamically select storage provider based on available environment variables
    
    Returns:
        CloudStorageProvider: Configured storage provider
    """
    try:
        if os.getenv('S3_BUCKET_NAME'):
            validate_env_vars('S3')
            return S3CompatibleProvider()
        else:
            validate_env_vars('GCP')
            return GCPStorageProvider()
    except ValueError as e:
        logger.error(f"Storage provider configuration error: {str(e)}")
        raise
