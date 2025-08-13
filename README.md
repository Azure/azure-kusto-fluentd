# azure-kusto-fluentd

[Fluentd](https://fluentd.org/) output plugin for ingesting logs and data into [Azure Data Explorer (Kusto)](https://azure.microsoft.com/en-us/services/data-explorer/).

## Overview

This plugin allows you to send data from Fluentd to Azure Data Explorer (Kusto) using Azure Blob Storage and Queue for scalable, reliable ingestion. It supports both buffered and non-buffered modes, handles authentication via Azure AD or Managed Identity, and provides robust error handling and logging.

## Requirements

- Ruby 2.5 or later  
  Check version (Windows/Linux):
  ```bash
  ruby --version
  ```
  Install on Ubuntu/Linux:
  ```bash
  sudo apt-get install ruby-full
  ```
  Install on Windows (using RubyInstaller):
  [Download RubyInstaller](https://rubyinstaller.org/)
  [Official Ruby installation guide](https://www.ruby-lang.org/en/documentation/installation/)

- Fluentd v1.0 or later  
  Check version (Windows/Linux):
  ```bash
  fluentd --version
  ```
  Install on Ubuntu/Linux:
  ```bash
  gem install fluentd
  ```
  Install on Windows (in Command Prompt after Ruby is installed):
  ```cmd
  gem install fluentd
  ```
  [Official Fluentd installation guide](https://docs.fluentd.org/installation)


## Installation

### RubyGems

```sh
$ gem install azure-kusto-fluentd
```

### Bundler

Add the following line to your Gemfile:

```ruby
gem "azure-kusto-fluentd"
```

And then execute:

```sh
$ bundle
```

## Azure Data Explorer (Kusto) Prerequisites
The _Kusto_ output plugin lets you ingest your logs into an [Azure Data Explorer](https://azure.microsoft.com/en-us/services/data-explorer/) cluster, using the [Queued Ingestion](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/about-kusto-ingest#queued-ingestion) mechanism.

## Ingest into Azure Data Explorer: create a Kusto cluster and database

Create an Azure Data Explorer cluster in one of the following ways:

* [Create a free-tier cluster](https://dataexplorer.azure.com/freecluster)
* [Create a fully featured cluster](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal)

## Create an Azure registered application

FluentD uses the Azure application's credentials to ingest data into your cluster.

* [Register an application](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app#register-an-application)
* [Add a client secret](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app#add-a-client-secret)
* [Authorize the app in your database](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/access-control/principals-and-identity-providers#azure-ad-tenants)

## Create a Managed Identity in Azure

- **System-assigned Managed Identity:**
  - [Enable system-assigned managed identity](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-enable-system-assigned-managed-identity)

- **User-assigned Managed Identity:**
  - [Create and assign user-assigned managed identity](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-create-user-assigned-managed-identity)

- **Grant Permissions:**
  - [Assign permissions to managed identity for Azure Data Explorer](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/access-control/principals-and-identity-providers)

## Workload Identity Authentication

- [Follow workload_identity.md](workload_identity.md)

## Create a database

To create a new database in your Azure Data Explorer cluster, use the following KQL command:

```kql
.create database <database_name>
```

## Create a table

Fluent Bit ingests the event data into Kusto in a JSON format. By default, the table includes 3 properties:

* `record` - the actual event payload.
* `tag` - the event tag.
* `timestamp` - the event timestamp.

A table with the expected schema must exist in order for data to be ingested properly.

```kql
.create table <table_name> (tag:string, timestamp:datetime, record:dynamic)
```

## Configuration parameters

| Key                                    | Description                                                                                                                                                                                                                                                             | Default                        |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `tenant_id`                            | The tenant/domain ID of the Azure Active Directory (AAD) registered application. Required if `managed_identity_client_id` isn't set.                                                                                                                                    | _none_                         |
| `client_id`                            | The client ID of the AAD registered application. Required if `managed_identity_client_id` isn't set.                                                                                                                                                                    | _none_                         |
| `client_secret`                        | The client secret of the AAD registered application ([App Secret](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#option-2-create-a-new-application-secret)). Required if `managed_identity_client_id` isn't set. | _none_                         |
| `managed_identity_client_id`           | The managed identity ID to authenticate with. Set to `SYSTEM` for system-assigned managed identity, or set to the MI client ID (`GUID`) for user-assigned managed identity. Required if `tenant_id`, `client_id`, and `client_secret` aren't set.                       | _none_                         |
| `endpoint`                   | The cluster's endpoint, usually in the form `https://cluster_name.region.kusto.windows.net`                                                                                                                                                            | _none_                         |
| `database_name`                        | The database name.                                                                                                                                                                                                                                                      | _none_                         |
| `table_name`                           | The table name.                                                                                                                                                                                                                                                         | _none_                         |
| `compression_enabled`                  | If enabled, sends compressed HTTP payload (gzip) to Kusto.                                                                                                                                                                                                              | `true`                         |
| `workers`                              | The number of [workers](../../../administration/multithreading#outputs) to perform flush operations for this output.                                                                                                                                                    | `0`                            |
| `buffered`                    | Enable buffering into disk before ingesting into Azure Kusto. If `buffered` is `true`, buffered mode is activated. If `false`, non-buffered mode is used.                                                                                                               | `true`                        |
| `delayed`                              | If `true`, enables delayed commit for buffer chunks. Only supported in buffered mode (`buffered` must be `true`). If `buffered` is `false`, delayed commit is not available.                                                     | `false`                        |
| `azure_cloud`                          | Azure cloud environment. E.g., `AzureCloud`, `AzureChinaCloud`, `AzureUSGovernmentCloud`, `AzureGermanCloud`.                                                                                                                    | `AzureCloud`                   |
| `chunk_keys` (buffer section)          | Only in buffered mode. Keys to use for chunking the buffer. Possible values: `tag`, `time`, or a combination such as `["tag", "time"]`. Controls how data is grouped and flushed.                                               | `["time"]`                    |
| `timekey` (buffer section)             | Only in buffered mode. Time interval for buffer chunking. Possible values: integer seconds (e.g., `60`, `3600`, `86400`).                                                                                                         | `86400` (1 day)                |
| `timekey_wait` (buffer section)        | Only in buffered mode. Wait time before flushing a timekey chunk after its time window closes. Possible values: duration string (e.g., `30s`, `5m`).                                                                             | `30s`                           |
| `timekey_use_utc` (buffer section)     | Only in buffered mode. Use UTC for timekey chunking. Possible values: `true`, `false`.                                                                                                                                           | `true`                          |
| `flush_at_shutdown` (buffer section)   | Only in buffered mode. Flush buffer at shutdown. Possible values: `true`, `false`.                                                                                                                                               | `true`                          |
| `retry_max_times` (buffer section)     | Only in buffered mode. Maximum number of retry attempts for buffer flush. Possible values: integer (e.g., `5`, `10`).                                                                                                            | `5`                             |
| `retry_wait` (buffer section)          | Only in buffered mode. Wait time between buffer flush retries. Possible values: duration string (e.g., `1s`, `10s`).                                                                                                             | `1s`                            |
| `overflow_action` (buffer section)     | Only in buffered mode. Action to take when buffer overflows. Possible values: `block`, `drop_oldest_chunk`, `throw_exception`.                                                            | `block`                         |
| `chunk_limit_size` (buffer section)    | Only in buffered mode. Maximum size per buffer chunk. Possible values: size string (e.g., `256m`, `1g`).                                                                                                                         | `256m`                          |
| `total_limit_size` (buffer section)    | Only in buffered mode. Maximum total buffer size. Possible values: size string (e.g., `2g`, `10g`).                                                                                                                              | `2g`                            |
| `flush_mode` (buffer section)          | Only in buffered mode. Buffer flush mode. Possible values: `interval`, `immediate`, `lazy`.                                                                                               | `interval`                      |
| `flush_interval` (buffer section)      | Only in buffered mode. Interval for buffer flush. Possible values: duration string (e.g., `10s`, `1m`).                                                                                                                          | `10s`                           |
| `logger_path`                           | Optional. File path for plugin log output. If not set, logs are written to stdout.                                                                                                                      | stdout(terminal)                        |
| `auth_type`                            | The authentication type to use. Possible values: `aad`, `user_managed_identity`, `system_managed_identity`,`workload_identity`.                                                                                                                                                                                                                                     | `aad`                        |
| `workload_identity_client_id`              | The client ID for Azure Workload Identity authentication. Required if using workload identity for authentication.                                                                                                               | _none_                         |
| `workload_identity_tenant_id`              | The tenant ID for Azure Workload Identity authentication. Required if using workload identity for authentication.                                                                                                               | _none_                         |
| `workload_identity_token_file`             | The file path to the token file for Azure Workload Identity authentication. Required if using workload identity for authentication.                                                                                             | `/var/run/secrets/azure/tokens/azure-identity-token`                        |

## Sample Configuration

```conf
<system>
  workers 1
</system>
<match test.kusto>
  @type kusto
  @log_level debug
  buffered true
  delayed false
  endpoint https://yourcluster.region.kusto.windows.net
  database_name your-db
  table_name your-table
  tenant_id <your-tenant-id>
  client_id <your-client-id>
  managed_identity_client_id SYSTEM
  compression_enabled true
  azure_cloud AzureCloud
  logger_path /var/log/azure-kusto-fluentd.log
  <buffer>
    @type memory
    # To chunk by tag only:
    # chunk_keys tag
    # To chunk by tag and time:
    # chunk_keys tag,time
    timekey 1m
    timekey_wait 30s
    timekey_use_utc true
    flush_at_shutdown true
    retry_max_times 5
    retry_wait 1s
    overflow_action block
    chunk_limit_size 256m
    total_limit_size 2g
    flush_mode interval
    flush_interval 10s
  </buffer>
</match>
```

# Fluentd Azure Data Explorer (Kusto) Output Plugin Architecture
![Architecture](architecture.png)

This diagram shows the main components and data flow for the plugin, including configuration, error handling, token management, and Azure resource interactions.

## Copyright

* License: Apache License, Version 2.0
