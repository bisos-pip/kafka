# -*- coding: utf-8 -*-

""" #+begin_org
* ~[Summary]~ :: =CS-Lib= The single source of the facts of this host's Kafka (and Kafbat UI) installation.
#+end_org """

""" #+begin_org
* [[elisp:(org-cycle)][| ~Description~ |]] :: Facts shared by kafka-sbom.pcs, kafkaAdmin.cs, quickKafka.cs,
kafka-sysd.pcs, kafbat-ui-sysd.pcs, kafka-here-dns.pcs and kafka-wvd.pcs: the account, the pinned
upstream versions and their checksums, the install/config/data/log paths, the EnvironmentFiles,
the fqdn and the three banna ports (broker, KRaft controller, Kafbat UI).
** Pure: importing this module has no side effects and touches no host state.
** The generators below return the text of /etc/kafka/server.properties, /etc/default/kafka,
/etc/default/kafbat-ui and of the shared [Service] lines of the units, so that the code that writes
them and the code that checks them (kafkaAdmin.cs -i driftCheck) cannot disagree.
** Status: Starting point.
** /[[elisp:(org-cycle)][| Planned Improvements |]]/ :
*** TODO Authentication for Kafbat UI (it is reachable only on loopback for now).
*** TODO Multi-node / remote clients: listenIp is loopback, so only this host can reach the broker.
#+end_org """

####+BEGIN: b:prog:file/proclamations :outLevel 1
""" #+begin_org
* *[[elisp:(org-cycle)][| Proclamations |]]* :: Libre-Halaal Software --- Part Of BISOS ---  Poly-COMEEGA Format.
** This is Libre-Halaal Software. © Neda Communications, Inc. Subject to AGPL.
** It is part of BISOS (ByStar Internet Services OS)
** Best read and edited  with Blee in Poly-COMEEGA (Polymode Colaborative Org-Mode Enhance Emacs Generalized Authorship)
#+end_org """
####+END:

####+BEGIN: b:prog:file/particulars :authors ("./inserts/authors-mb.org")
""" #+begin_org
* *[[elisp:(org-cycle)][| Particulars |]]* :: Authors, version
** This File: /bisos/git/bxRepos/bisos-pip/kafka/py3/bisos/kafka/kafkaInfo.py
** Authors: Mohsen BANAN, http://mohsen.banan.1.byname.net/contact
#+end_org """
####+END:

import dataclasses
import pathlib

from bisos.banna import tcpPorts as bannaTcpPorts


def _bannaPortNu(portName: str) -> int:
    """The registry, bisos.banna, is authoritative: never hardcode these ports."""
    return bannaTcpPorts.tcpPortsAssignedList.tcpPortsList[portName].portNu


@dataclasses.dataclass(frozen=True)
class KafkaInfo:
    """The facts of this host's Kafka installation. Built once, as kafkaInfo below."""

    # The OS account both units run as. A system account with no login shell.
    acctName: str = "kafka"

    # Apache Kafka is not packaged by Debian: the upstream binary tarball is pinned
    # here, with the SHA-512 Apache publishes next to it. Bumping the version means
    # bumping the checksum too --- the sbom refuses a tarball that does not match.
    version: str = "4.3.1"
    scalaVersion: str = "2.13"
    distSha512: str = (
        "c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e"
        "8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c"
    )

    # Kafbat UI (github.com/kafbat/kafka-ui) ships as one Spring Boot jar.
    # The SHA-256 is the digest GitHub records for the release asset.
    kafbatVersion: str = "1.5.0"
    kafbatSha256: str = "8bebff7b21ddb084b5b647e271136f7d97f46da6c7bc70f9cd47775dfbd3c10e"

    # librdkafka, the C client library under the confluent-kafka Python package, built from
    # source with Kerberos (GSSAPI) because the PyPI wheels omit it (see KafkaUsePython.org).
    # The SHA-256 is that of GitHub's archive of the tag; no upstream signature is checked.
    # The Python binding must be at least as new as this library: confluent-kafka X needs
    # librdkafka >= X, so confluentKafkaVersion is pinned to the same number.
    librdkafkaVersion: str = "2.16.0"
    librdkafkaSha256: str = "e6b61de61d3282879a88e4ee3d9f634a8b05bf78a9198dfc25c4820c0c9ed231"
    confluentKafkaVersion: str = "2.16.0"

    # apt packages to build librdkafka and the binding, and to use Kerberos at run time.
    clientBuildPkgs: tuple = (
        "build-essential", "pkg-config", "python3-dev",
        "libssl-dev", "libsasl2-dev", "libzstd-dev", "liblz4-dev", "libcurl4-openssl-dev", "zlib1g-dev",
    )
    clientKerberosPkgs: tuple = ("libsasl2-modules-gssapi-mit", "krb5-user")

    # Binaries: root-owned, read-only for the account. One dir per version, plus
    # a "current" symlink, so an upgrade is unpack + flip and a rollback is a flip.
    installBase: pathlib.Path = pathlib.Path("/opt/kafka")

    confDir: pathlib.Path = pathlib.Path("/etc/kafka")

    # KRaft metadata and the partition logs. Owned by the account.
    dataDir: pathlib.Path = pathlib.Path("/var/lib/kafka/data")

    # server.log, controller.log, ... (Kafka's application logs, not partition data).
    logDir: pathlib.Path = pathlib.Path("/var/log/kafka")

    # Read by systemd as root before the units drop to User=. root:acctName 640.
    envFile: pathlib.Path = pathlib.Path("/etc/default/kafka")
    kafbatEnvFile: pathlib.Path = pathlib.Path("/etc/default/kafbat-ui")

    # Single node, combined broker+controller (KRaft; Kafka 4 has no ZooKeeper).
    nodeId: int = 1

    # Every listener binds loopback only: the broker is for this host's tenants and
    # Kafbat UI has no authentication. nginx (kafka.here) reaches Kafbat over loopback.
    listenIp: str = "127.0.0.1"

    # The cluster name Kafbat UI displays.
    clusterName: str = "local"

    heapOpts: str = "-Xms512m -Xmx1g"
    kafbatJavaOpts: str = "-Xmx512m"

    # Kafbat UI is what humans browse, as http://kafka.here
    fqdn: str = "kafka.here"

    # The Debian package of the JRE. Kafka 4.x needs Java 17+, but Kafbat UI 1.5.0 is built
    # for Java 25 (class file 69): 21 failed with UnsupportedClassVersionError. One JRE for both.
    jrePkg: str = "openjdk-25-jre-headless"

    portNu: int = dataclasses.field(default_factory=lambda: _bannaPortNu('kafka'))
    controllerPortNu: int = dataclasses.field(default_factory=lambda: _bannaPortNu('kafkaController'))
    kafbatPortNu: int = dataclasses.field(default_factory=lambda: _bannaPortNu('kafbatUi'))

    @property
    def distName(self) -> str:
        return f"kafka_{self.scalaVersion}-{self.version}"

    @property
    def distUrl(self) -> str:
        """archive.apache.org keeps every release; downloads.apache.org only the latest few."""
        return f"https://archive.apache.org/dist/kafka/{self.version}/{self.distName}.tgz"

    @property
    def distDir(self) -> pathlib.Path:
        return self.installBase / self.distName

    @property
    def homeLink(self) -> pathlib.Path:
        """installBase/current -> distDir. Units and CLI go through this, never through distDir."""
        return self.installBase / "current"

    @property
    def binDir(self) -> pathlib.Path:
        return self.homeLink / "bin"

    def binPath(self, tool: str) -> pathlib.Path:
        """E.g. binPath("kafka-topics") -> /opt/kafka/current/bin/kafka-topics.sh"""
        return self.binDir / f"{tool}.sh"

    @property
    def serverProps(self) -> pathlib.Path:
        return self.confDir / "server.properties"

    @property
    def metaProps(self) -> pathlib.Path:
        """Written by kafka-storage.sh format. Its presence means storage is formatted."""
        return self.dataDir / "meta.properties"

    @property
    def kafbatDir(self) -> pathlib.Path:
        return self.installBase / "kafbat"

    @property
    def kafbatJar(self) -> pathlib.Path:
        return self.kafbatDir / f"api-v{self.kafbatVersion}.jar"

    @property
    def kafbatUrl(self) -> str:
        return (f"https://github.com/kafbat/kafka-ui/releases/download/"
                f"v{self.kafbatVersion}/api-v{self.kafbatVersion}.jar")

    @property
    def librdkafkaDir(self) -> pathlib.Path:
        return self.installBase / f"librdkafka-{self.librdkafkaVersion}"

    @property
    def librdkafkaHome(self) -> pathlib.Path:
        """installBase/librdkafka -> librdkafkaDir. Builds of the Python binding go through this."""
        return self.installBase / "librdkafka"

    @property
    def librdkafkaUrl(self) -> str:
        return f"https://github.com/confluentinc/librdkafka/archive/refs/tags/v{self.librdkafkaVersion}.tar.gz"

    @property
    def librdkafkaBuildEnvFile(self) -> pathlib.Path:
        """A shell snippet, sourced before pip builds confluent-kafka against this library."""
        return self.librdkafkaDir / "build-env.sh"

    @property
    def bootstrapServers(self) -> str:
        """What every client --- CLI, Kafbat UI, tenants --- connects to."""
        return f"{self.listenIp}:{self.portNu}"

    @property
    def controllerAddr(self) -> str:
        return f"{self.listenIp}:{self.controllerPortNu}"

    @property
    def baseUrl(self) -> str:
        """The address humans browse, through the nginx vhost."""
        return f"http://{self.fqdn}"

    @property
    def kafbatLocalUrl(self) -> str:
        """Straight to Kafbat UI, bypassing nginx."""
        return f"http://{self.listenIp}:{self.kafbatPortNu}"

    def cli(self, tool: str) -> str:
        """A by-hand CLI invocation with --bootstrap-server already supplied.

        The client tools need no account: they talk to the broker over TCP and
        touch no files under dataDir.
        """
        return f"{self.binPath(tool)} --bootstrap-server {self.bootstrapServers}"


# A Singleton
kafkaInfo = KafkaInfo()


def serverPropertiesContent(info: KafkaInfo = kafkaInfo) -> str:
    """The content /etc/kafka/server.properties is generated with. Written only when absent.

    Single node in KRaft combined mode. Every replication factor is 1: with one
    broker, the stock value of 3 for the internal topics would leave
    __consumer_offsets unable to be created, and every consumer group would hang.
    controller.quorum.bootstrap.servers (not controller.quorum.voters) is what
    kafka-storage.sh format --standalone expects.
    """
    return (
        "# Generated by kafka-sbom.pcs from bisos.kafka.kafkaInfo. Written only when absent.\n"
        "process.roles=broker,controller\n"
        f"node.id={info.nodeId}\n"
        f"controller.quorum.bootstrap.servers={info.controllerAddr}\n"
        f"listeners=PLAINTEXT://{info.bootstrapServers},CONTROLLER://{info.controllerAddr}\n"
        f"advertised.listeners=PLAINTEXT://{info.bootstrapServers},CONTROLLER://{info.controllerAddr}\n"
        "controller.listener.names=CONTROLLER\n"
        "inter.broker.listener.name=PLAINTEXT\n"
        "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n"
        f"log.dirs={info.dataDir}\n"
        "num.partitions=1\n"
        "offsets.topic.replication.factor=1\n"
        "transaction.state.log.replication.factor=1\n"
        "transaction.state.log.min.isr=1\n"
        "share.coordinator.state.topic.replication.factor=1\n"
        "share.coordinator.state.topic.min.isr=1\n"
        "log.retention.hours=168\n"
    )


def envFileContent(info: KafkaInfo = kafkaInfo) -> str:
    """The content /etc/default/kafka is generated with. Written only when absent.

    LOG_DIR is read by kafka-run-class.sh. Without it the broker writes its
    application logs under the root-owned distDir and fails to start.
    """
    return (
        f'KAFKA_HEAP_OPTS="{info.heapOpts}"\n'
        f"LOG_DIR={info.logDir}\n"
        "# Add any overrides (e.g. KAFKA_JVM_PERFORMANCE_OPTS) here\n"
    )


def kafbatEnvFileContent(info: KafkaInfo = kafkaInfo) -> str:
    """The content /etc/default/kafbat-ui is generated with. Written only when absent.

    Kafbat UI is a Spring Boot application: every setting is also an environment
    variable. SERVER_ADDRESS keeps it on loopback --- it has no authentication.
    DYNAMIC_CONFIG_ENABLED=false keeps the cluster list fixed to the one here
    rather than editable from the browser.
    """
    return (
        f"SERVER_ADDRESS={info.listenIp}\n"
        f"SERVER_PORT={info.kafbatPortNu}\n"
        f"KAFKA_CLUSTERS_0_NAME={info.clusterName}\n"
        f"KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS={info.bootstrapServers}\n"
        "DYNAMIC_CONFIG_ENABLED=false\n"
        "# Add any overrides (e.g. AUTH_TYPE, LOGGING_LEVEL_ROOT) here\n"
    )


def librdkafkaBuildEnvContent(info: KafkaInfo = kafkaInfo) -> str:
    """Environment under which pip builds confluent-kafka from source against our librdkafka.

    The rpath makes the built extension find the library without LD_LIBRARY_PATH, so the
    resulting pipx or venv install is self-contained at run time.
    """
    home = info.librdkafkaHome
    return (
        "# Generated by kafka-sbom.pcs from bisos.kafka.kafkaInfo.\n"
        "# Use:  . " + str(home / "build-env.sh") + "\n"
        f"#       pip install --no-binary confluent-kafka confluent-kafka=={info.confluentKafkaVersion}\n"
        f"export C_INCLUDE_PATH={home}/include${{C_INCLUDE_PATH:+:$C_INCLUDE_PATH}}\n"
        f"export LIBRARY_PATH={home}/lib${{LIBRARY_PATH:+:$LIBRARY_PATH}}\n"
        f'export LDFLAGS="-Wl,-rpath,{home}/lib ${{LDFLAGS:-}}"\n'
        "export PIP_NO_BINARY=confluent-kafka\n"
        "# pip caches built wheels: a cached one built against another librdkafka would be reused silently.\n"
        "export PIP_NO_CACHE_DIR=1\n"
    )


def sysdServiceLines(envFile: pathlib.Path, info: KafkaInfo = kafkaInfo) -> str:
    """The identity lines every kafka unit has in its [Service] section."""
    return (
        f"User={info.acctName}\n"
        f"Group={info.acctName}\n"
        f"EnvironmentFile={envFile}\n"
    )


def _kvMap(text: str) -> dict[str, str]:
    """KEY=VALUE lines of a properties or EnvironmentFile; comments and blank lines are skipped."""
    pairs: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        pairs[key.strip()] = value.strip()
    return pairs


def kvDrift(liveText: str, generatedText: str) -> dict[str, list]:
    """Compare a live KEY=VALUE file with what its generator would produce.

    The files are written only when absent, so hand overrides are legitimate and later
    generator lines never reach existing hosts. This therefore only reports:
      missing -- keys the generator has and the live file lacks
      differs -- (key, expected, live) where both have the key with different values
      extra   -- keys only the live file has (hand additions)
    """
    expected = _kvMap(generatedText)
    live = _kvMap(liveText)
    return {
        "missing": [k for k in expected if k not in live],
        "differs": [(k, expected[k], live[k]) for k in expected if k in live and live[k] != expected[k]],
        "extra": [k for k in live if k not in expected],
    }


def unitDrift(unitText: str, envFile: pathlib.Path, info: KafkaInfo = kafkaInfo) -> list[str]:
    """The identity lines (sysdServiceLines) that an installed unit file lacks."""
    present = {line.strip() for line in unitText.splitlines()}
    return [line for line in sysdServiceLines(envFile, info).splitlines() if line not in present]


####+BEGIN: b:py3:cs:framework/endOfFile :basedOn "classification"
""" #+begin_org
* [[elisp:(org-cycle)][| *End-Of-Editable-Text* |]] :: emacs and org variables and control parameters
#+end_org """

#+STARTUP: showall

### local variables:
### no-byte-compile: t
### end:
####+END:
