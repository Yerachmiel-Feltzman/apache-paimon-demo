.PHONY: setup cleanup spin_up_iceberg_rest_catalog shutdown_iceberg_rest_catalog

setup: 
	chmod +x setup.sh
	./setup.sh

cleanup:
	chmod +x cleanup.sh
	./cleanup.sh

spin_up_iceberg_rest_catalog:
	docker compose -f docker-compose.yaml up iceberg-rest-catalog -d 

shutdown_iceberg_rest_catalog:
	docker compose -f docker-compose.yaml down

run_paimon_and_iceberg_cross_platform_demo:
	python ./demos/paimon-and-iceberg-cross-platform.py

run_paimon_only_demo:
	python ./demos/paimon-only.py
