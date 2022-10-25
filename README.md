# pipeline2.0

# commands
start orion: prefect orion start
reset database: prefect orion database reset -y
start queue: prefect agent start --work-queue "test_queue"
deploy: prefect deployment build yclib/main.py:main_flow -n oberon -q test_queue
apply deploy: prefect deployment apply /home/playground/main_flow-deployment.yaml