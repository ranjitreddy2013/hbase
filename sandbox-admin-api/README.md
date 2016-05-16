# Sandbox Admin Tool and API

The Sandbox Admin Tool invokes the API to manage the lifecycle operations of a sandbox. This includes operations such as
 creation, push (commit changes to the original), manage proxy tables (transparent to the user) and delete sandbox 
 tables.

## Deploy

Compile with maven ( mvn clean install -P philips-sandbox ).

Deploy artifact (e.g. sandbox-admin-api-0.98.12-mapr-1506.jar ) to /opt/mapr/lib and scripts to /usr/bin/ .

