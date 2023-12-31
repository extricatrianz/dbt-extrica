
  
# dbt-extrica

The ```dbt-extrica``` adapter allows users to interact with [Extrica's](https://www.extrica.ai)
Trino Engine, a distributed SQL query engine, using dbt. 
This adapter is designed to facilitate the use of dbt for transforming and modeling data within Extrica.
#### Features
- **Extrica Compatibility:** Compatible with Extrica's Trino Query Engine, allowing users to leverage dbt within Extrica.
- **JWT Authentication:** Utilizes JWT for secure authentication with Extrica. The adapter handles the generation of JWT tokens behind the scenes via username and password configured in profiles.yml.
- Extrica eliminates **complex data-engineering** and de-couples migrations to help businesses experience a quantum leap in the insights. 

## Description 
#### About DBT 
dbt (data build tool) is a powerful data transformation workflow tool that facilitates quick and collaborative deployment of analytics code. It follows software engineering best practices such as modularity, continuous integration/deployment (CI/CD), testing, and documentation. With dbt, individuals familiar with SQL can easily build production-grade data pipelines.

#### Connecting to Multiple Data Sources
> <b> Challenges in Limited Connectivity </b> <br>
> Default configurations in dbt make it challenging to connect to databases beyond the primary one associated with the project.
> Many organizations deal with diverse data sources, such as Oracle, Snowflake, SQL Server, Google BigQuery, Delta Lake, Iceberg, AWS Redshift, Vertica, Azure Synapse, MongoDB, AWS S3 and other data sources.
> Integrating and transforming data from these varied sources efficiently becomes essential for comprehensive analytics.

#### Solution: Leveraging Trino and Catalogs in Extrica
Extrica's Trino is an advanced query engine that excels in federated queries across multiple data sources and also allows the writeback capabilities to variety of data sources. 
Its ability to connect to various databases and process SQL queries at scale makes it an ideal solution for organizations dealing with diverse data sources.

<b>Extrica</b>, built on Trino introduces the concept of catalogs to address the challenge of connecting to multiple data sources seamlessly within dbt.
Each catalog corresponds to a specific data source, enabling a unified approach to managing and transforming data across various systems.

## Connecting to Extrica

#### Example profiles.yml 
Here is a  example of a dbt-extrica profile parameters. At a minimum, you need to specify `type`, `method`, `username`, `password` `host`, `port`, `schema`, `catalog` and `threads`.
<File name='~/.dbt/profiles.yml'>

```yaml
<profile-name>:
  outputs:
    dev:
      type: extrica
      method: jwt 
      username: [username for jwt auth]
      password: [password for jwt auth]  
      host: [extrica hostname]
      port: [port number]
      schema: [dev_schema]
      catalog: [catalog_name]
      threads: [1 or more]

    prod:
      type: extrica
      method: jwt 
      username: [username for jwt auth]
      password: [password for jwt auth]  
      host: [extrica hostname]
      port: [port number]
      schema: [dev_schema]
      catalog: [catalog_name]
      threads: [1 or more]
  target: dev

```
</File>

#### Description of Profile Fields

| Parameter  | Type     | Description                              |
|------------|----------|------------------------------------------|
| type       | string  | Specifies the type of dbt adapter (Extrica). |
| method     | jwt      | Authentication method for JWT authentication. |
| username   | string   | Username for JWT authentication. The obtained JWT token is used to initialize a trino.auth.JWTAuthentication object.      |
| password   | string   | Password for JWT authentication. The obtained JWT token is used to initialize a trino.auth.JWTAuthentication object.      |
| host       | string   | The host parameter specifies the hostname or IP address of the Trino server.           |
| port       | integer  | The port parameter specifies the port number on which the Trino server is listening.        |
| schema     | string   | Schema or database name for the connection. |
| catalog    | string   | Name of the catalog representing the data source. |
| threads    | integer  | Number of threads for parallel execution of queries. (1 or more |

## Getting Started
#### Install dbt-extrica adapter

```sh
 pip install  dbt-extrica
```
#### Initialize dbt project 
```sh
dbt init
```
* <b> Select Adapters </b> : After running the `dbt init` command, you'll be prompted to select adapters from a list of available adapters. Choose the appropriate adapter for your project.
* <b> Modify Profiles.yml </b> : The dbt init command will create a project structure in current dir and a .dbt folder inside users of your system. Inside .dbt folder, you'll find a profiles.yml file.

#### Configure Profiles.yml
* Open the profiles.yml file in a text editor.
* Locate the section for your selected adapter and project (e.g., extrica).
* Add the necessary connection details such as host, port, user, password, etc.
* Save and close the profiles.yml file.
#### Check connection is successful 
```sh
dbt debug
```
#### To run all models use below command
```sh
dbt run
```
