# dbt from scratch 🌟 a Consid session

> An end-to-end, containerized data engineering project that leverages open-source technology to generate, transform, load and visualize data. Everything runs on `Docker` with official images and `Dev Containers`.
> 

# 1️⃣ Introduction

What we will use:

- `Docker` to containerize our application.
- A slim Python `devcontainer` with some configurations to skip the need to install anything locally.
- `dbt` to transform data. We’ll also mock EL with Python scripts and the `dbt seed` command.
- An official `PostgreSQL` image as our open-source relational database (mock-DWH). Easiest to setup with `dbt` as it has an official provider.
- `DBeaver` as database UI. Other options could be `psql` (CLI), `adminer` or `pgAdmin`.
- `Metabase` as our open-source, free and self-hosted on Docker visualization tool.

What we won’t use:

- An orchestrator/workflow scheduler. Examples of this would be the paid `dbt Cloud` offering, `Azure Data Factory`, `Airflow`, `Prefect`, `Dagster`, `Cron jobs`, `Kestra`, `Databricks workflows`. We won’t use this because they either cost money or are too complex to setup properly for a local project. In a real-life scenario, most probably as a consultant, `dbt Cloud` would be used. I might add open-source `Dagster` in the future.
- `git` can be added whenever you want.

Prerequisites to follow along:

- `Docker Desktop`

# 2️⃣ Init

Open a terminal and create a folder that our project will live in. Change into that directory and start the Docker initialization. This will create three files for us, and we will overwrite two of them. We will get a `.dockerignore` file for free.

```bash
mkdir consid_dbt && \
cd consid_dbt && \
mkdir .devcontainer && \
touch .devcontainer/devcontainer.json && \
touch requirements.txt && \
docker init
```

Hit enter, selecting “Other”. Open directory with `vscode` here.

```bash
code .
```

Edit the `Dockerfile` with this content.

```docker
#Dockerfile

FROM mcr.microsoft.com/vscode/devcontainers/python:3.10

WORKDIR /usr/src/consid_dbt

ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN if [ "$USER_GID" != "1000" ] || [ "$USER_UID" != "1000" ]; then \
        groupmod --gid $USER_GID vscode \
        && usermod --uid $USER_UID --gid $USER_GID vscode; \
    fi

COPY requirements.txt /tmp/pip-tmp/

RUN pip3 --disable-pip-version-check \
        --use-deprecated=legacy-resolver \
        --no-cache-dir \
        install -r /tmp/pip-tmp/requirements.txt && \
        rm -rf /tmp/pip-tmp

ENV DBT_PROFILES_DIR=/usr/src/consid_dbt
```

And edit the `compose.yml` with this:

```yaml
version: "3.9"

services:
  consid_postgres:
    container_name: consid_postgres
    image: postgres:15.2-alpine
    environment:
      - POSTGRES_PASSWORD=postgres
    ports:
      - "8000:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5
    volumes:
      - consid_dbt:/var/lib/postgresql/data

  consid_dbt:
    container_name: consid_dbt
    build:
      context: .
      dockerfile: Dockerfile
      args:
        # On Linux, you may need to update USER_UID and USER_GID below 
        # if not your local UID is not 1000.
        USER_UID: 1000
        USER_GID: 1000
    image: consid_dbt
    volumes:
      - .:/usr/src/consid_dbt:cached
    depends_on:
      consid_postgres:
        condition: service_healthy
    # Overrides default command so things don't shut down after the process ends.
    command: sleep infinity
    
    # Runs app on the same network as the database container, allows "forwardPorts" in devcontainer.json function.
    network_mode: service:consid_postgres

    # Uncomment the next line to use a non-root user for all processes.
    user: vscode

  metabase:
    image: metabase/metabase
    container_name: metabase
    ports:
      - "3000:3000"
    network_mode: service:consid_postgres

volumes:
  consid_dbt:
```

Edit the `requirements.txt` with this:

```python
sqlfluff==2.3.5
sqlfluff-templater-dbt==2.3.5
dbt-postgres==1.7.2
pandas==2.1.3
```

`sqlfluff` is a linting tool that works great with `dbt` . `dbt-postgres` will also install `dbt-core` and `pandas` is used in some Python scripts later on. 

Edit the `devcontainer.json` with this:

```json
// Update the VARIANT arg in docker-compose.yml to pick a Python version: 3, 3.8, 3.7, 3.6
{
    "name": "consid_dbt",
    "dockerComposeFile": "../compose.yml",
    "service": "consid_dbt",
    "workspaceFolder": "/usr/src/consid_dbt",
    "customizations": {
        "vscode": {
            "settings": {
                "terminal.integrated.defaultProfile.linux#": "/bin/zsh",
                "python.pythonPath": "/usr/local/bin/python",
                "python.languageServer": "Pylance",
                "files.associations": {
                    "*.sql": "jinja-sql"
                },
                "sqltools.connections": [
                    {
                        "name": "Database",
                        "driver": "PostgreSQL",
                        "previewLimit": 50,
                        "server": "localhost",
                        "port": 5432,
                        "database": "postgres",
                        "username": "postgres",
                        "password": "postgres"
                    }
                ],
                "sql.linter.executablePath": "sqlfluff",
                "sql.format.enable": false
            },
            "extensions": [
                "bastienboutonnet.vscode-dbt",
                "dorzey.vscode-sqlfluff",
                "editorconfig.editorconfig",
                "innoverio.vscode-dbt-power-user",
                "ms-azuretools.vscode-docker",
                "ms-python.python",
                "ms-python.vscode-pylance",
                "visualstudioexptteam.vscodeintellicode",
                "eamodio.gitlens",
                "mtxr.sqltools-driver-pg",
                "mtxr.sqltools",
                "redhat.vscode-yaml",
                "samuelcolvin.jinjahtml"
            ]
        }
    },
    // Comment the if git is not initialized.
    "initializeCommand": "git submodule update --init",
    "remoteUser": "vscode"
}
```

Open a terminal and type:

```bash
docker compose build && docker compose up -d
```

This will create a container, with two containers inside. One is our database, and one is a Python 3.10 image with `dbt-core` + `dbt-postgres` adapter installed.

To start working inside the container, where `dbt` is installed, we can utilize our pre-configured `devcontainer.json` file to “Reopen in container”. Hit `ctrl+shift+p` and search for this.

Now it’s time to let `dbt` do some scaffolding for us. We will create a new `dbt` project inside the current directory. This will create a new project inside a subdirectory named what we call the project. I did not like this setup as it would mean this folder structure → `consid_dbt/consid_dbt`. Also, the default location for `profiles.yml` is at `~/.dbt` which is good for local projects, but then each developer needs to place this file outside of the repository on their local machine. By excluding it from the initialization and adding it manually inside the repo (and using `jinja` to inject variables from an `.env` file not to expose sensitive values) we make it possible to share this project with anyone easily. However, the credentials is not sensitive here so we will not hide them.

```bash
dbt init consid_dbt --skip-profile-setup && \
mv consid_dbt/* . && \
wait && \
rm -r consid_dbt && \
touch profiles.yml && \
touch packages.yml
```

This sets up `dbt` to connect to our PostgreSQL. Paste this into the `profiles.yml`: 

```yaml
# profiles.yml

consid_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: consid_postgres
      user: postgres
      password: postgres
      port: 5432
      dbname: postgres
      schema: public
      threads: 1
```

Paste this into the `dbt_project.yml`:

```yaml
#dbt_project.yml

name: 'consid_dbt'
version: '1.0.0'
config-version: 2

profile: 'consid_dbt'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"

models:
  consid_dbt:
    staging:
      +materialized: view
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
```

And this on `packages.yml` with this:

```yaml
#packages.yml

packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

# 3️⃣ `dbt` setup

Test the connection to our `PostgreSQL` container by running:

```bash
dbt debug
```

Install packages as defined in the `packages.yml`:

```bash
dbt deps
```

## 🌱 Seeds

Right now our repo doesn’t look like much. Let’s create `seeds`.

`Seeds` are typically an easy way to populate data that typically don’t change, are not part of any source and would help in semantically clarify other data. A good example would be the table `state_codes` as below:

```yaml
+------+------------+
| code | state      |
+------+------------+
| AL   | Alabama    |
| AK   | Alaska     |
| AZ   | Arizona    |
| AR   | Arkansas   |
| CA   | California |
+------+------------+
```

However, we will use it to mock an EL flow to our `bronze` schema. It will be a simple Python script that will act as an EL-tool, such as Azure Data Factory, that fetches new `logins` from a source system and loads it into our `bronze` layer. This layer will be referenced to by `dbt` as a `source`.

## 📜 Scripts

Create a folder called scripts, one file called `login_generator_script.py` and one file called `people_generator_script.py`.

```bash
mkdir scripts && \
touch scripts/login_generator_script.py && \
touch scripts/people_generator_script.py
```

Paste the following script for the `login_generator_script.py`:

```python
#!/usr/bin/env python

import pandas as pd
import hashlib
import datetime
import random
import os

# Function to generate an MD5 hash of the current timestamp
# Makes sure that each ID will be unique.
unique_id_counter = 1

# Function to get the current timestamp
def get_current_timestamp():
    return str(datetime.datetime.now())

def generate_unique_id():
    global unique_id_counter
    timestamp = get_current_timestamp()
    encode_input = f"{timestamp}_{unique_id_counter}"
    encoded = encode_input.encode()
    unique_id = hashlib.md5(encoded).hexdigest()
    unique_id_counter += 1
    return unique_id

# Function to get the day of the week as an integer (1=Monday, 7=Sunday)
def get_day_of_week():
    return datetime.datetime.today().isoweekday()

def generate_random_userid():
    return random.randint(1,5)

num_rows = 100

# Dict to store data
data = {
    "id": [generate_unique_id() for _ in range(num_rows)],
    "logintimestamp": [get_current_timestamp() for _ in range(num_rows)],
    "dayofweek": [get_day_of_week() for _ in range(num_rows)],
    "userid": [generate_random_userid() for _ in range(num_rows)]
}

# Set the working directory to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# File to store the data
csv_file = "../seeds/raw_logins.csv"

# Check if the file already exists
file_exists = os.path.isfile(csv_file)

df = pd.DataFrame(data)

# Append the data to the CSV file
if file_exists:
    df.to_csv(csv_file, mode='a', header=False, index=False)
else:
    df.to_csv(csv_file, index=False)

print(df)
```

And this for `people_generator_script.py`:

```python
#!/usr/bin/env python

import pandas as pd
import datetime
import os

data = {
    "id": [1,2,3,4],
    "firstname": ["Jakob", "Stefan", "Rami", "Therese"],
    "lastname": ["Agelin", "Verzel", "Moghrabi", "Olsson"],
    "created_at": [None for _ in range(4)],
    "updated_at": [None for _ in range(4)],
    "deleted_at": [None for _ in range(4)]
}

# Set the working directory to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# File to store the data
csv_file = "../seeds/raw_people.csv"

# Check if the file already exists
file_exists = os.path.isfile(csv_file)

# Load existing data from the CSV file
existing_data = pd.read_csv(csv_file, keep_default_na=True) if file_exists else pd.DataFrame()

df = pd.DataFrame(data)

# Set created_at timestamp only if the file is new
if not file_exists:
    df['created_at'] = str(datetime.datetime.now())
    df.to_csv(csv_file, index=False)
    

# Check if row exists in the source file and update updated_at
if not existing_data.empty and pd.Series(df['id']).isin(existing_data['id']).any():
    df = pd.DataFrame(existing_data)
    df.loc[df["deleted_at"].isnull(), 'updated_at'] = str(datetime.datetime.now())
    df.to_csv(csv_file, index=False)

print(df)
```

Run both scripts once. By using `chmod` (change mode) in bash, we change the permission for this file with `x` meaning it can get executed by just stating the file name. We could run it anyway with `python3 some_script.py` but this is cooler.

```python
chmod +x scripts/login_generator_script.py && \
scripts/login_generator_script.py && \
chmod +x scripts/people_generator_script.py && \
scripts/people_generator_script.py
```

Now, run `dbt seed`.

```bash
dbt seed
```

Good to know that the compiled query can be viewed at `target/run/consid_dbt/seeds` for each seed. There we can see that each seed is run with `truncate` first (if it doesn’t exist). We will only see the latest run here.

If we need to change schema of our source data, with seeds, we can add the `-f` flag for a full-refresh of the target table. In this case, as we can see in the logs, we can see that the statement then changes from `truncate` to `drop table if exists`.

```bash
dbt seed -f
```

Login to DBeaver and see the results with a query.

```sql
SELECT id, firstname, lastname, created_at, updated_at, deleted_at
FROM public.raw_people;
```

## 🧊 Models and sources

A `model` is a `select`-statement in SQL that will compile into `DML` and `DDL` queries in the target data warehouse. They will create tables, views or nothing at all (ephemeral). `dbt` supports `Jinja templating`, and offers a modular approach to development in SQL. A `source` is a table inside the data warehouse or lakehouse that already has been populated with data by another workflow. It is a reference to that raw table, and by specifying a `sources.yml` file we allow `dbt` to reference these sources when the code compiles.

Let’s improve our `dbt` project with three layers → `staging`, `intermediate` and `marts`. `dbt` suggests the following structure.

```
jaffle_shop
├── README.md
├── analyses
├── seeds
│   └── employees.csv
├── dbt_project.yml
├── macros
│   └── cents_to_dollars.sql
├── models
│   ├── intermediate
│   │   └── finance
│   │       ├── _int_finance__models.yml
│   │       └── int_payments_pivoted_to_orders.sql
│   ├── marts
│   │   ├── finance
│   │   │   ├── _finance__models.yml
│   │   │   ├── orders.sql
│   │   │   └── payments.sql
│   │   └── marketing
│   │       ├── _marketing__models.yml
│   │       └── customers.sql
│   ├── staging
│   │   ├── jaffle_shop
│   │   │   ├── _jaffle_shop__docs.md
│   │   │   ├── _jaffle_shop__models.yml
│   │   │   ├── _jaffle_shop__sources.yml
│   │   │   ├── base
│   │   │   │   ├── base_jaffle_shop__customers.sql
│   │   │   │   └── base_jaffle_shop__deleted_customers.sql
│   │   │   ├── stg_jaffle_shop__customers.sql
│   │   │   └── stg_jaffle_shop__orders.sql
│   │   └── stripe
│   │       ├── _stripe__models.yml
│   │       ├── _stripe__sources.yml
│   │       └── stg_stripe__payments.sql
│   └── utilities
│       └── all_dates.sql
├── packages.yml
├── snapshots
└── tests
    └── assert_positive_value_for_total_amount.sql
```

Let’s use this, and start with `staging`:

```bash
mkdir models/staging && \
mkdir models/staging/login_service && \
touch models/staging/login_service/_login_service__models.yml && \
touch models/staging/login_service/_login_service__sources.yml && \
touch models/staging/login_service/stg_login_service__logins.sql && \
touch models/staging/login_service/stg_login_service__people.sql && \

mkdir models/staging/login_service/base && \
touch models/staging/login_service/base/base_login_service__people.sql && \
touch models/staging/login_service/base/base_login_service__deleted_people.sql
rm -r models/example
```

Update the `.yml` files:

```yaml
#_login_service__models.yml

version: 2

models:
  - name: stg_login_service__people
    description: Customer data
    columns:
      - name: people_id
        tests:
          - unique
          - not_null

  - name: stg_login_service__logins
    columns:
      - name: login_id
        tests:
          - unique
          - not_null

      - name: day_of_week
        tests:
          - accepted_values:
              values: ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
```

```yaml
#_login_service__sources.yml

version: 2

sources:
  - name: login_service
    schema: dbt_jakob
    description: Login data for the Login Service
    tables:
      - name: raw_people
        description: One record per person that has logged in
      - name: raw_logins
        description: One record per login that a person has made
        freshness:
          warn_after: {count: 24, period: hour}
        loaded_at_field: "logintimestamp::timestamp"
```

Paste this into `stg_login_service__logins.sql`:

```sql
--stg_login_service__logins.sql

{{
  config(
    materialized = 'view'
    )
}}

with source as (

    select * from {{ source('login_service', 'raw_logins') }}

),

renamed as (
    select
        id::text as login_id,
        logintimestamp::timestamp as login_timestamp,
        userid as people_id,
        case dayofweek
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
            when 7 then 'Sunday'
            else 'Unknown'
        end as day_of_week
    from source
)

select * from renamed
```

Paste this into `stg_login_service__people.sql`:

```sql
--stg_login_service__people.sql

{{
  config(
    materialized = 'view'
    )
}}

with people as (

    select * from {{ ref('base_login_service__people') }}
),

deleted_people as (

    select * from {{ ref('base_login_service__deleted_people') }}
),

join_and_mark_deleted_people as (

    select
        people.*,
        coalesce(deleted_people.deleted_at is not null, false) as is_deleted
    from people
    left join deleted_people on people.people_id = deleted_people.people_id
)

select * from join_and_mark_deleted_people
```

Paste this into `base_login_service__deleted_people.sql`:

```sql
--base_login_service__deleted_people.sql

{{
  config(
    materialized = 'view'
    )
}}

with source as (

    select * from {{ source('login_service', 'raw_people') }}
),

deleted_customers as (

    select
        id as people_id,
        deleted_at
    from source
)

select * from deleted_customers
```

Paste this into `base_login_service__people.sql`:

```sql
--base_login_service__people.sql

{{
  config(
    materialized = 'view'
    )
}}

with source as (

    select * from {{ source('login_service', 'raw_people') }}
),

renamed as (

    select
        id as people_id,
        concat(firstname, ' ', lastname) as full_name,
        updated_at
    from source
)

select * from renamed
```

## 🐇 `sqlfluff`

Notice how the `sqlfluff` plugin is complaining about a rule. It wants us to “Select wildcards then simple targets before calculations and aggregates”. Another default rule is to not allow a file to end without a empty new line. In this case I don’t want this specific rule to apply. Let’s change this behavior by adding a `.sqlfluff` file and add the following code to it.

```bash
touch .sqlfluff
```

```toml
[sqlfluff]
templater = dbt
dialect = postgres
exclude_rules = L034, L009

[sqlfluff:templater:jinja]
apply_dbt_builtins = True
load_macros_from_path = macros

# If using the dbt templater, we recommend setting the project dir.
[sqlfluff:templater:dbt]
project_dir = ./
profiles_dir = ./
profile = consid_dbt
target = dev

[sqlfluff:rules:ambiguous.column_references]  # Accept number in group by
group_by_and_order_by_style = implicit
```

Run the project to see the views created by `dbt`:

```bash
dbt run
```

View the compiled code and queries in either `target/` or `logs/`. Notice how `sqlfluff` complains about the compiled code inside target. Add a `.sqlfluffignore` file to the project:  

```bash
touch .sqlfluffignore
```

```
reports
target
dbt_packages
macros
```

## 🔜 Intermediate

The reason for an intermediate layer is to prepare staging data into marts. It could very well be `ephemeral` which means that they will not create any object in the target data warehouse - the query will only run as a CTE and is able to be reference by the `ref` command in `dbt`. It is beneficial to use a separate layer for these queries as it will reduce complexity of queries and promote modularity in the code.

Create the `intermediate` layer:

```bash
mkdir models/intermediate && \
touch models/intermediate/_int_marketing__models.yml && \
touch models/intermediate/int_logins_pivoted_to_people.sql
```

Paste this into `_int_marketing__models.yml`:

```yaml
#_int_marketing__models.yml

version: 2

models:
  - name: int_logins_pivoted_to_people
    description: Calculate the number of logins per people
    columns:
      - name: people_id
      - name: login_amount
```

Paste this into `int_logins_pivoted_to_people.sql`:

```sql
--int_logins_pivoted_to_people.sql

with logins as (
    select * from {{ ref('stg_login_service__logins') }}
),

pivot_and_aggregate_logins_to_people_grain as (

    select
        people_id,
        count(login_id) as login_amount
    from logins
    group by 1
)

select * from pivot_and_aggregate_logins_to_people_grain
```

Create the `marts` layer, together with a `snapshot` for SCD2 (`people` table):

```bash
mkdir snapshots && \
touch snapshots/people_history.sql && \

mkdir models/marts && \
mkdir models/marts/marketing && \
touch models/marts/marketing/_marketing_models.yml && \
touch models/marts/marketing/logins.sql && \
touch models/marts/marketing/people.sql

```

Paste this into `people_history.sql`:

```sql
--people_history.sql

{% snapshot people_history %}

{{
   config(
       target_database='postgres',
       target_schema='temporal_data',
       unique_key='people_id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

select * from {{ ref('stg_login_service__people') }}

{% endsnapshot %}
```

Paste this into `_marketing_models.yml`:

```yaml
#_marketing_models.yml

version: 2
models:
  - name: people
    columns:
      - name: people_id
        description: Primary key of the people table
        tests:
          - unique
          - not_null
      - name: full_name
        description: The full name.
      - name: updated_at
        description: The date and time when the person's record was last updated.
          This is in the standard timestamp format.
      - name: is_deleted
        description: Indicates whether the person's record has been marked as deleted.
          This is a true or false value.
      - name: name_length
        description: The number of characters in the person's full name. This is a
          whole number.
      - name: login_amount
        description: The total number of times the person has logged in. This is a
          whole number.
    description: The dbt model 'people' is a tool that organizes and analyzes user
      data. It tracks whether a user's account is active, the number of times they've
      logged in, the length of their name, and the last time their data was updated.
      This model can be used to understand user behavior, such as how often they log
      in and if there's a correlation between name length and login frequency. This
      information can help in making data-driven decisions, like tailoring user engagement
      strategies.
  - name: logins
    description: One record per login.
    columns:
      - name: login_id
        description: Primary key of the login.
        tests:
          - unique
          - not_null
      - name: login_timestamp
        description: When the user logged in.
      - name: day_of_week
        description: Number indicating the dayOfWeek. 1 = Monday.
      - name: people_id
        description: Foreign key for peopleId. Should be renamed in public schema.
        tests:
          - not_null
          - relationships:
              to: ref('people')
              field: people_id
      - name: full_name
        description: Full name of people
      - name: _dbt_hash
        description: A unique identifier for each login record, generated using a
          hash function that combines several columns to ensure uniqueness.
      - name: _dbt_inserted_at
        description: The timestamp when this login record was first added to the database.
      - name: _dbt_updated_at
        description: The timestamp when this login record was last modified in the
          database.
      - name: login_amount
        description: The total number of times a person has logged in. This is a running
          count that increments by 1 each time the person logs in.
```

Paste this into `logins.sql`:

```sql
--logins.sql

{{
  config(
    materialized = 'incremental',
    unique_key = 'login_id',
    on_schema_change = 'append_new_columns',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['_dbt_inserted_at']
    )
}}

with logins as (
    select * from {{ ref("stg_login_service__logins") }}
),

people as (
    select * from {{ ref("stg_login_service__people") }}
),

rename as (
    select
        logins.login_id,
        logins.login_timestamp,
        logins.day_of_week,
        logins.people_id,
        people.full_name
    from logins
    left join people on logins.people_id = people.people_id
),

final as (
    select
        *,
        {{ 
            dbt_utils.generate_surrogate_key(
                dbt_utils.get_filtered_columns_in_relation(
                        from=ref("stg_login_service__logins")
                )
            )
        }} as _dbt_hash,
        current_timestamp as _dbt_inserted_at,
        current_timestamp as _dbt_updated_at
    from rename
)

select * from final

{% if is_incremental() %}

    where _dbt_hash not in (select _dbt_hash from {{ this }})

{% endif %}
```

Paste this into `people.sql`:

```sql
--people.sql

{{
  config(
    materialized = 'table',
    )
}}

with people as (
    select
        *,
        length(full_name) as name_length
    from {{ ref("stg_login_service__people") }}
),

logins_pivoted_to_people as (
    select * from {{ ref('int_logins_pivoted_to_people') }}
),

final as (

    select
        people.*,
        logins_pivoted_to_people.login_amount
    from people
    left join
        logins_pivoted_to_people
        on people.people_id = logins_pivoted_to_people.people_id
)

select * from final
```

## 👁️‍🗨️ Metabase

Open meta base at `[localhost:3000](http://localhost:3000)` and follow the setup. For `host` and `port` supply the network that we set up in the `compose.yml` → `consid_postgres` and `5432`. This differs from how we connect to the database from our laptop, as we are then connecting to the database with `localhost` and the other exposed port `8000`. Explore how many logins our users has done each!