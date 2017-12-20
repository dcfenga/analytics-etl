# spark-gerrit-analytics-etl
Spark ETL to extra analytics data from Gerrit Projects.

Job can be launched with the following parameters:

```
bin/spark-submit \
    --conf spark.es.nodes=es.mycompany.com \
    --conf spark.es.net.http.auth.user=elastic \
    --conf spark.es.net.http.auth.pass=changeme \
    $JARS/SparkAnalytics-assembly-1.0.jar \
    --since 2000-06-01 \
    --aggregate email_hour \
    --url http://gerrit.mycompany.com \
    -e gerrit/analytics
```
### Parameters
- since, until, aggregate are the same defined in Gerrit Analytics plugin
    see: https://gerrit.googlesource.com/plugins/analytics/+/master/README.md
- -u --url Gerrit server URL with the analytics plugins installed
- -p --prefix (*optional*) Projects prefix. Limit the results to those projects that start with the specified prefix.
- -e --elasticIndex specify as <index>/<type> to be loaded in Elastic Search
    if not provided no ES export will be performed
- -o --out folder location for storing the output as JSON files
    if not provided data is saved to </tmp>/analytics-<NNNN> where </tmp> is
    the system temporary directory
- -a --email-aliases (*optional*) "emails to author alias" input data path.

  CSVs with 3 columns are expected in input.

  Here an example of the required files structure:
  ```csv
  author,email,organization
  John Smith,john@email.com,John's Company
  John Smith,john@anotheremail.com,John's Company
  David Smith,david.smith@email.com,Indipendent
  David Smith,david@myemail.com,Indipendent
  ```

  You can use the following command to quickly extract the list of authors and emails to create part of an input CSV file:
  ```bash
  echo -e "author,email\n$(git log --pretty="%an,%ae%n%cn,%ce"|sort |uniq )" > /tmp/my_aliases.csv
  ```
  Once you have it, you just have to add the organization column.

  *NOTE:*
  * **organization** will be extracted from the committer email if not specified
  * **author** will be defaulted to the committer name if not specified

## Development environment

A docker compose file is provided to spin up an instance of Elastisearch with Kibana locally.
Just run `docker-compose up`.

Kibana will run on port `5601` and Elastisearch on port `9200`

### Default credentials

The Elastisearch default user is `elastic` and the default password `changeme`

### Caveats

If Elastisearch dies with `exit code 137` you might have to give Docker more memory ([check this article for more details](https://github.com/moby/moby/issues/22211))