# dbt from scratch 🌟 a Consid session

# Introduction

What we will use:

- `Docker` to containerize our application.
- A slim Python `devcontainer` with some configurations to skip the need to install anything locally.
- `dbt` to transform data. We’ll also mock EL with Python scripts and the `dbt seed` command.
- An official `PostgreSQL` image as our open-source relational database (mock-DWH). Easiest to setup with `dbt` as it has an official provider.
- `DBeaver` as database UI. Other options could be `psql` (CLI), `adminer` or `pgAdmin`.

What we won’t use:

- An orchestrator/workflow scheduler. Examples of this would be the paid dbt Cloud offering, Azure Data Factory, Airflow, Prefect, Dagster, Cron jobs, Kestra, Databricks workflows. We won’t use this because they either cost money or are too complex to setup properly for a local project. In a real-life scenario, most probably as a consultant, dbt Cloud would be used.
- `git` can be added whenever you want.

Prerequisites to follow along:

- `Docker`

# Init

Open a terminal and create a folder that our project will live in. Change into that directory and start the docker initialization. This will create three files for us, and we will overwrite two of them. We will get a `.dockerignore` file for free.

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
  
volumes:
  consid_dbt:
```

Edit the `requirements.txt` with this:

```python
sqlfluff==0.10.1
dbt-postgres
pandas==1.2.3
```

`sqlfluff` is a linting tool that works great with `dbt` . `dbt-postgres` will also install `dbt-core` and pandas is used in some Python scripts later on. 

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
    // Uncomment the git initialization for this demo (no repo initialized).
    //"initializeCommand": "git submodule update --init",
    "remoteUser": "vscode"
}
```

Open a terminal and type:

```bash
docker compose build && docker compose up -d
```

This will create a container, with two containers inside. One is our database, and one is a Python 3.10 image with `dbt-core` + `dbt-postgres` adapter installed.

To start working inside the container, where `dbt` is installed, we can utilize our pre-configured `devcontainer.json` file to “Reopen in container”. Hit `ctrl+shift+p` and search for this.

Now it’s time to let `dbt` do some scaffolding for us. We will create a new dbt project inside the current directory. This will create a new project inside a subdirectory named what we call the project. I did not like this setup as it would mean this folder structure → `consid_dbt/consid_dbt`. Also, the default location for `profiles.yml` is at `~/.dbt` which is good for local projects, but then each developer needs to place this file outside of the repository on their local machine. By excluding it from the initialization and adding it manually inside the repo (and using `jinja` to inject variables from an `.env` file not to expose sensitive values) we make it possible to share this project with anyone easily. However, the credentials is not sensitive here so we will not hide them.

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
      host: localhost
      user: postgres
      password: postgres
      port: 5432
      dbname: postgres
      schema: public
      threads: 1
```

And this on `packages.yml` with this:

```yaml
#packages.yml

packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

# `dbt` setup

Test the connection to our PostgreSQL container by running:

```bash
dbt debug
```

Install packages as defined in the packages.yml:

```bash
dbt deps
```

## Seeds

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

## Scripts

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

## Models and sources

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
#stg_login_service__logins.sql

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
#stg_login_service__people.sql

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
#base_login_service__deleted_people.sql

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
#base_login_service__people.sql

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

Notice how the `sqlfluff` plugin is complaining about a rule. It wants us to “Select wildcards then simple targets before calculations and aggregates”. Another default rule is to not allow a file to end without a empty new line. In this case I don’t want this specific rule to apply. Let’s change this behavior by adding a `.sqlfluff` file and add the following code to it.

```bash
touch .sqlfluff
```

```toml
[sqlfluff]
exclude_rules = L034, L009
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