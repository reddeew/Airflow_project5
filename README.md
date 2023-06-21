# Airflow_project5
Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Air􀃓ow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


Execution :

Connections aws_credentials and redshift must be created before running the DAG.

Under Connections, select Create.

On the create connection page, enter the following values:

Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier

Once you've entered these values, select Save and Add Another.

On the next create connection page, enter the following values:

Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end.
Schema: Enter dev. This is the Redshift database you want to connect to.
Login: Enter the login name you created when launching your Redshift cluster.
Password: Enter the password you created when launching your Redshift cluster.
Port: Enter 5439.
Once you've entered these values, select Save.

set path
export PYTHONPATH=/home/workspace/airflow/plugins:$PYTHONPATH
copy the dag,plugins folders  to airflow directory 
and run /opt/airflow/start.sh


