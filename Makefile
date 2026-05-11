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
	source .venv/bin/activate && python ./demos/paimon-and-iceberg-cross-platform.py

run_paimon_only_demo:
	source .venv/bin/activate && python ./demos/paimon-only.py

run_paimon_only_notebook:
	source .venv/bin/activate && jupyter notebook ./demos/paimon-only.ipynb

run_paimon_and_iceberg_cross_platform_notebook:
	source .venv/bin/activate && jupyter notebook ./demos/paimon-and-iceberg-cross-platform.ipynb
