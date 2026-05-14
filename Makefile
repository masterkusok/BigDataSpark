init:
	mkdir -p ./jars/
	curl -L \
		https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0-patch3/clickhouse-jdbc-0.6.0-patch3-all.jar \
		-o ./jars/clickhouse.jar \
		;

	curl -L \
		https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
		-o ./jars/postgresql.jar \
		;

pg-job:
	docker exec -it \
		bdlab_spark \
    	spark-submit \
  		--jars /extra-jars/postgresql.jar \
  		/home/jovyan/work/spark-jobs/pg_normalize.py \
	;

ch-job:
	docker exec -it \
		bdlab_spark \
    	spark-submit \
  		--jars /extra-jars/clickhouse.jar,/extra-jars/postgresql.jar \
  		/home/jovyan/work/spark-jobs/clickhouse_reports.py \
	;