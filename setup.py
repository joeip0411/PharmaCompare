from setuptools import find_packages, setup

setup(
    name="dagster_project",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dagster_project": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-postgres",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-core<1.9",
        "dbt-snowflake<1.9",
        "boto3",
        "supabase==2.7.4",
        "pandas==2.2.3",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)