import os
import json
import logging
from azure.identity import ClientSecretCredential
from azure.mgmt.powerbidedicated import PowerBIDedicated
from azure.mgmt.powerbiembedded import PowerBIEmbeddedManagementClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Azure AD and Microsoft Fabric API configuration for source subscription
source_client_id = os.getenv('SOURCE_CLIENT_ID')
source_client_secret = os.getenv('SOURCE_CLIENT_SECRET')
source_tenant_id = os.getenv('SOURCE_TENANT_ID')
source_subscription_id = os.getenv('SOURCE_SUBSCRIPTION_ID')
source_workspace_name = os.getenv('SOURCE_WORKSPACE_NAME')
source_resource_group = os.getenv('SOURCE_RESOURCE_GROUP')

# Azure AD and Microsoft Fabric API configuration for target subscription
target_client_id = os.getenv('TARGET_CLIENT_ID')
target_client_secret = os.getenv('TARGET_CLIENT_SECRET')
target_tenant_id = os.getenv('TARGET_TENANT_ID')
target_subscription_id = os.getenv('TARGET_SUBSCRIPTION_ID')
target_workspace_name = os.getenv('TARGET_WORKSPACE_NAME')
target_resource_group = os.getenv('TARGET_RESOURCE_GROUP')

def get_credentials(client_id, client_secret, tenant_id):
    logging.info(f"Authenticating with Azure AD tenant {tenant_id}")
    credentials = ClientSecretCredential(
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id
    )
    return credentials

def get_powerbi_client(credentials, subscription_id):
    logging.info(f"Initializing Power BI Embedded Management Client for subscription {subscription_id}")
    powerbi_client = PowerBIEmbeddedManagementClient(credentials, subscription_id)
    return powerbi_client

def export_workspace_contents(powerbi_client, resource_group, workspace_name):
    logging.info(f"Exporting contents from workspace '{workspace_name}' in resource group '{resource_group}'")
    try:
        # Assuming you have methods to list datasets, reports, dashboards, etc.
        datasets = powerbi_client.datasets.list_by_workspace(resource_group, workspace_name)
        reports = powerbi_client.reports.list_by_workspace(resource_group, workspace_name)

        # Convert to lists and store in a dictionary
        workspace_contents = {
            'datasets': [dataset.as_dict() for dataset in datasets],
            'reports': [report.as_dict() for report in reports],
            # Add other object types as needed
        }

        # Save to JSON files
        for content_type, content_list in workspace_contents.items():
            file_name = f"{content_type}_export.json"
            with open(file_name, 'w') as f:
                json.dump(content_list, f, indent=4)
            logging.info(f"Exported {len(content_list)} {content_type} to {file_name}")

        return workspace_contents

    except HttpResponseError as e:
        logging.error(f"Failed to export contents: {e}")
        return None

def import_workspace_contents(powerbi_client, resource_group, workspace_name, workspace_contents):
    logging.info(f"Importing contents into workspace '{workspace_name}' in resource group '{resource_group}'")
    try:
        # Read contents from JSON files
        for content_type, content_list in workspace_contents.items():
            logging.info(f"Importing {len(content_list)} {content_type}")
            for content in content_list:
                # Implement the import logic for each content type
                if content_type == 'datasets':
                    # Example: Create the dataset
                    # Note: Adjust the method calls as per the SDK documentation
                    powerbi_client.datasets.create(
                        resource_group_name=resource_group,
                        workspace_name=workspace_name,
                        dataset_name=content['name'],
                        parameters=content
                    )
                elif content_type == 'reports':
                    # Example: Create the report
                    powerbi_client.reports.create(
                        resource_group_name=resource_group,
                        workspace_name=workspace_name,
                        report_name=content['name'],
                        parameters=content
                    )
                # Add other object types as needed
                logging.info(f"Imported {content_type[:-1]}: {content['name']}")

    except HttpResponseError as e:
        logging.error(f"Failed to import contents: {e}")

def main():
    # Source credentials and client
    source_credentials = get_credentials(source_client_id, source_client_secret, source_tenant_id)
    source_powerbi_client = get_powerbi_client(source_credentials, source_subscription_id)

    # Target credentials and client
    target_credentials = get_credentials(target_client_id, target_client_secret, target_tenant_id)
    target_powerbi_client = get_powerbi_client(target_credentials, target_subscription_id)

    # Export contents from source workspace
    workspace_contents = export_workspace_contents(
        source_powerbi_client,
        source_resource_group,
        source_workspace_name
    )

    if workspace_contents:
        # Import contents into target workspace
        import_workspace_contents(
            target_powerbi_client,
            target_resource_group,
            target_workspace_name,
            workspace_contents
        )
        logging.info("Data migration completed successfully.")
    else:
        logging.error("Data migration failed due to errors in exporting contents.")

if __name__ == "__main__":
    main()
