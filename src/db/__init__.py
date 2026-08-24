"""
The Postgres sink: what the cleaned frame becomes on the way into the database.

    settings   connection details, read from the environment
    contract   which frame columns become which table columns, and as what
    writer     the staging-table load and the upsert that merges it
    migrate    applying sql/schema.sql

Split three ways because only one of them needs a database to test. ``contract``
is a pure description -- a list of names and casts -- so the question "does the
pipeline still produce everything the table requires" is answerable in
milliseconds, without a JVM and without Postgres. That question is the one that
breaks when a stage is edited, and a check that needs a running container to
answer it is a check that stops being run.

Re-exporting nothing, following ``src.spark``: ``writer`` holds a function
named ``write`` and ``migrate`` one named ``migrate``, and binding both the
module and the function on the package makes which one you get depend on
import order.
"""
