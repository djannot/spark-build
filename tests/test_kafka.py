import logging
import os
import pytest
import json
import shakedown
import subprocess

from tests import s3, utils

import sdk_auth
import sdk_hosts
import sdk_install


LOGGER = logging.getLogger(__name__)
PRODUCER_SERVICE_NAME = "Spark->Kafka Producer"
GENERIC_HDFS_USER_PRINCIPAL = "client@{realm}".format(realm=sdk_auth.REALM)

DEFAULT_KAFKA_TASK_COUNT=3
KERBERIZED_KAFKA = False
KAFKA_KRB5="W2xpYmRlZmF1bHRzXQpkZWZhdWx0X3JlYWxtID0gTE9DQUwKCltyZW" \
           "FsbXNdCiAgTE9DQUwgPSB7CiAgICBrZGMgPSBrZGMubWFyYXRob24u" \
           "YXV0b2lwLmRjb3MudGhpc2Rjb3MuZGlyZWN0b3J5OjI1MDAKICB9Cg=="
# Currently using stub-universe so it's beta-kafka, for the last time
KAFKA_PACKAGE_NAME = "beta-kafka"
KAFKA_SERVICE_NAME = "secure-kafka" if KERBERIZED_KAFKA else "kafka"


def setup_module(module):
    utils.require_spark()
    utils.upload_file(os.environ["SCALA_TEST_JAR_PATH"])


def teardown_module(module):
    utils.teardown_spark()
    sdk_install.uninstall(KAFKA_PACKAGE_NAME, KAFKA_SERVICE_NAME)


def _kerberized_kafka():
    fqdn = "{service_name}.{host_suffix}".format(service_name=KAFKA_SERVICE_NAME,
                                                 host_suffix=sdk_hosts.AUTOIP_HOST_SUFFIX)

    brokers = ["kafka-0-broker", "kafka-1-broker", "kafka-2-broker"]

    principals = []
    for b in brokers:
        principals.append("kafka/{instance}.{domain}@{realm}".format(
            instance=b,
            domain=fqdn,
            realm=sdk_auth.REALM))

    principals.append("client@{realm}".format(realm=sdk_auth.REALM))

    kerberos_env = sdk_auth.KerberosEnvironment()
    kerberos_env.add_principals(principals)
    kerberos_env.finalize()

    service_kerberos_options = {
        "service": {
            "name": KAFKA_SERVICE_NAME,
            "security": {
                "kerberos": {
                    "enabled": True,
                    "kdc_host_name": kerberos_env.get_host(),
                    "kdc_host_port": int(kerberos_env.get_port()),
                    "keytab_secret": kerberos_env.get_keytab_path(),
                }
            }
        }
    }

    sdk_install.uninstall(KAFKA_PACKAGE_NAME, KAFKA_SERVICE_NAME)
    sdk_install.install(
        KAFKA_PACKAGE_NAME,
        KAFKA_SERVICE_NAME,
        DEFAULT_KAFKA_TASK_COUNT,
        additional_options=service_kerberos_options,
        timeout_seconds=30 * 60)


def _kafka():
    LOGGER.info("Ensuring unsecure KAFKA is installed")

    sdk_install.uninstall(KAFKA_PACKAGE_NAME, KAFKA_SERVICE_NAME)
    sdk_install.install(
        KAFKA_PACKAGE_NAME,
        KAFKA_SERVICE_NAME,
        DEFAULT_KAFKA_TASK_COUNT,
        additional_options={"service": {}},
        timeout_seconds=30 * 60)


@pytest.mark.sanity
@pytest.mark.runnow
def test_spark_and_kafka():
    def producer_launched():
        return utils.streaming_job_launched(PRODUCER_SERVICE_NAME)

    def producer_started():
        return utils.streaming_job_running(PRODUCER_SERVICE_NAME)

    def kafka_broker_dns():
        cmd = "dcos {package_name} --name={service_name} endpoints broker".format(
            package_name=KAFKA_PACKAGE_NAME, service_name=KAFKA_SERVICE_NAME)
        try:
            stdout = subprocess.check_output(cmd, shell=True).decode('utf-8')
        except Exception as e:
            raise e("Failed to get broker endpoints")

        return json.loads(stdout)["dns"][0]

    def install_kafka():
        if KERBERIZED_KAFKA:
            LOGGER.warn("Using secure Kerberized Kafka cluster")
            _kerberized_kafka()
        else:
            LOGGER.warn("Using insecure Kafka cluster")
            _kafka()

    if utils.kafka_enabled():
        install_kafka()
        kerberos_flag = "true" if KERBERIZED_KAFKA else "false"  # flag for using kerberized kafka given to app
        stop_count = "48"  # some reasonable number
        broker_dns = kafka_broker_dns()
        topic = "top1"

        big_file, big_file_url = "/mnt/mesos/sandbox/big.txt", "http://norvig.com/big.txt"

        # arguments to the application
        producer_args = " ".join([broker_dns, big_file, topic, kerberos_flag])

        uris = "spark.mesos.uris=http://norvig.com/big.txt"
        if KERBERIZED_KAFKA:
            uris += ",http://s3-us-west-2.amazonaws.com/arand-sandbox-mesosphere/client-jaas-executor-grover.conf"

        common_args = [
            "--conf", "spark.mesos.containerizer=mesos",
            "--conf", "spark.scheduler.maxRegisteredResourcesWaitingTime=2400s",
            "--conf", "spark.scheduler.minRegisteredResourcesRatio=1.0",
            "--conf", uris
        ]

        kerberos_args = [
            "--conf", "spark.mesos.driver.secret.names=__dcos_base64___keytab",
            "--conf", "spark.mesos.driver.secret.filenames=kafka-client.keytab",
            "--conf", "spark.mesos.executor.secret.names=__dcos_base64___keytab",
            "--conf", "spark.mesos.executor.secret.filenames=kafka-client.keytab",
            "--conf", "spark.mesos.task.labels=DCOS_SPACE:/spark",
            "--conf", "spark.executorEnv.KRB5_CONFIG_BASE64={}".format(KAFKA_KRB5),
            "--conf", "spark.mesos.driverEnv.KRB5_CONFIG_BASE64={}".format(KAFKA_KRB5),
            "--conf", "spark.driver.extraJavaOptions=-Djava.security.auth.login.config="
                      "/mnt/mesos/sandbox/client-jaas-executor-grover.conf",
            "--conf", "spark.executor.extraJavaOptions="
                      "-Djava.security.auth.login.config=/mnt/mesos/sandbox/client-jaas-executor-grover.conf",
            "--conf", "spark.mesos.uris="
                      "http://s3-us-west-2.amazonaws.com/arand-sandbox-mesosphere/client-jaas-executor-grover.conf,"
                      "http://norvig.com/big.txt"
        ]

        producer_config = ["--conf", "spark.cores.max=2", "--conf", "spark.executor.cores=2",
                           "--class", "KafkaProducer"] + common_args

        if KERBERIZED_KAFKA:
            producer_config += kerberos_args

        producer_id = utils.submit_job(app_url=utils._scala_test_jar_url(),
                                       app_args=producer_args,
                                       app_name="/spark",
                                       args=producer_config)

        shakedown.wait_for(lambda: producer_launched(), ignore_exceptions=False, timeout_seconds=600)
        shakedown.wait_for(lambda: producer_started(), ignore_exceptions=False, timeout_seconds=600)

        consumer_config = ["--conf", "spark.cores.max=4", "--class", "KafkaConsumer"] + common_args

        if KERBERIZED_KAFKA:
            consumer_config += kerberos_args

        consumer_args = " ".join([broker_dns, topic, stop_count, kerberos_flag])

        utils.run_tests(app_url=utils._scala_test_jar_url(),
                        app_args=consumer_args,
                        expected_output="Read {} words".format(stop_count),
                        app_name="/spark",
                        args=consumer_config)

        utils.kill_driver(producer_id, "/spark")

