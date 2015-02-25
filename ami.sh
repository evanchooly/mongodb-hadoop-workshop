#!/bin/sh

PACKAGES=""

function log() {
	echo "[----------- $* -----------]"
}

function add() {
	while [ "$1" ]
	do
		PACKAGES="${PACKAGES} $1"
		shift
	done
}

function mongodb() {
	sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
	echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
	add mongodb-org
}

function jdk() {
	sudo add-apt-repository -y ppa:webupd8team/java
	echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
	add oracle-java8-installer
	if [ -z "`grep JAVA_HOME ~/.profile`" ]
	then
		log "Setting JAVA_HOME"
		echo "export JAVA_HOME=/usr/lib/jvm/java-8-oracle" >> ~/.profile
		echo "export JAVA_HOME=/usr/lib/jvm/java-8-oracle" | sudo tee -a /etc/profile
	fi
}

function install() {
	log Installing ${PACKAGES}
	sudo apt-get -qq update
	sudo apt-get install -y  ${PACKAGES}
}

function install_hadoop() {
	if [ -z "`grep HADOOP_PREFIX ~/.profile`" ]
	then
		log "Setting HADOOP_PREFIX"
		echo "export HADOOP_PREFIX=/usr/local/hadoop" >> ~/.profile
		echo "export PATH=\$PATH:\$HADOOP_PREFIX/bin:\$HADOOP_PREFIX/sbin" >> ~/.profile
	fi
	source ~/.profile
	cd /tmp
	HADOOP_VERSION=2.6.0
	[ ! -f hadoop-${HADOOP_VERSION}.tar.gz ] && wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

	sudo rm -rf /usr/local/hadoop /tmp/hadoop-ubuntu
	tar xzf hadoop-${HADOOP_VERSION}.tar.gz
	sudo mv hadoop-${HADOOP_VERSION} /usr/local/hadoop

	cd /usr/local/hadoop/etc/hadoop
	echo "export JAVA_HOME=/usr/lib/jvm/java-8-oracle" >> hadoop-env.sh
	cat <<EOF > core-site.xml
<configuration>
	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://localhost:9000</value>
	</property>
</configuration>
EOF
	cat <<EOF > hdfs-site.xml
<configuration>
	<property>
		<name>dfs.replication</name>
		<value>1</value>
  	</property>
</configuration>
EOF

	hdfs namenode -format -force

	cd /usr/local/hadoop/sbin
	./start-dfs.sh
	./start-yarn.sh
}

function install_hive() {
	log "Installing hive"
	if [ -z "`grep HIVE_HOME ~/.profile`" ]
	then
		log "Setting HIVE_HOME"
		echo "export HIVE_HOME=/usr/local/hive" >> ~/.profile
		echo "export PATH=\$PATH:\$HIVE_HOME/bin" >> ~/.profile
	fi
	source ~/.profile
	HIVE_VERSION=1.0.0
	cd /tmp
	[ ! -f apache-hive-${HIVE_VERSION}-bin.tar.gz ] && wget https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz
	tar xzf apache-hive-${HIVE_VERSION}-bin.tar.gz

	sudo rm -rf /usr/local/hive
	sudo mv apache-hive-${HIVE_VERSION}-bin /usr/local/hive

	hadoop fs -mkdir       /tmp
	hadoop fs -mkdir       -p /user/hive/warehouse
	hadoop fs -chmod g+w   /tmp
	hadoop fs -chmod g+w   /user/hive/warehouse
	hadoop fs -mkdir -p hdfs://localhost:9000/usr/local/hive/lib/
	hadoop fs -put /usr/local/hive/lib/hive-builtins-0.10.0.jar hdfs://localhost:9000/usr/local/hive/lib/hive-builtins-0.10.0.jar

	hive &> /tmp/hive.out &
	log "Done installing hive"
}

function ssh_config() {
	cd ~/.ssh
	cat <<EOF > ~/.ssh/config
Host localhost
HostName localhost
User ubuntu
EOF
	grep -v ubuntu authorized_keys > keys.tmp
	mv keys.tmp authorized_keys
	rm -f id_rsa*
	ssh-keygen -q -t rsa -f /home/ubuntu/.ssh/id_rsa -N ""
	cat id_rsa.pub >> authorized_keys
	
	ssh localhost ls .profile
	ssh 0:0:0:0:0:0:0:1 ls .profile
}

function install_pig() {
	log "Installing pig"
	cd /tmp
	PIG_VERSION=0.13.0
	[ ! -f pig-${PIG_VERSION}.tar.gz ] && wget http://apache.arvixe.com/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz
	tar xzf pig-${PIG_VERSION}.tar.gz
	sudo rm -rf /usr/local/pig
	sudo mv pig-${PIG_VERSION} /usr/local/pig
	if [ -z "`grep PIG_HOME ~/.profile`" ]
	then
		log "Setting PIG_HOME"
		echo "export PIG_HOME=/usr/local/pig" >> ~/.profile
		echo "export PATH=\$PATH:\$PIG_HOME/bin" >> ~/.profile
	fi
	log "Done installing pig"
}

function install_workshop() {
	log "Installing workshop project files"
	cd
	[ ! -d mongodb-hadoop-workshop ] && git clone https://github.com/evanchooly/mongodb-hadoop-workshop.git
	cd mongodb-hadoop-workshop
	git pull
	git checkout noanswers

	cd dataset
	tar zxf mlsmall.tar.gz
	mongorestore --drop -d movielens mlsmall/
	
	cd ..
	mvn compile clean
	#if [ ! -f ml-10m.zip ] 
	#then 
		#wget http://files.grouplens.org/datasets/movielens/ml-10m.zip
		#unzip -o ml-10m.zip
		#mvn compile exec:java
	#fi
}

function install_spark {
	cd /tmp
	SPARK_VERSION=1.2.1
	[ ! -f spark-${SPARK_VERSION}-bin-hadoop2.4.tgz ] && wget http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}-bin-hadoop2.4.tgz
	tar xzf spark-${SPARK_VERSION}-bin-hadoop2.4.tgz
	sudo rm -rf /usr/local/spark
	sudo mv spark-${SPARK_VERSION}-bin-hadoop2.4 /usr/local/spark
	if [ -z "`grep SPARK_HOME ~/.profile`" ]
	then
		log "Setting SPARK_HOME"
		echo "export SPARK_HOME=/usr/local/spark" >> ~/.profile
		echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.profile
	fi
	cd ~/mongodb-hadoop-workshop

	hadoop fs -mkdir hdfs://localhost:9000/movielens/
	hadoop fs -put dataset/mlsmall/movies.bson hdfs://localhost:9000/movielens/
	log "Done installing spark"
}

function run_examples {
	cd
	. .profile

	cd ~/mongodb-hadoop-workshop/mapreduce
	mvn package
	log "Running mapreduce example"
	time hadoop jar target/mapreduce-1.0-SNAPSHOT.jar \
		com.mongodb.workshop.MapReduceExercise \
		mongodb://127.0.0.1:27017/movielens.ratings \
		mongodb://127.0.0.1:27017/movielens.movies \
		update=true

	cd ../pig
	mvn clean package
	log "Running pig example"
	time pig PigExercise.pig

	cd ../hive
	mvn clean package
	cp target/libs/mongo-*.jar /usr/local/hive/lib/
	cp target/libs/mongo-*.jar /usr/local/hadoop/share/hadoop/common/lib/
	time hive -f HiveExercise.q 

	cd ../spark
	mvn clean package
	time spark-submit --master local \
      --driver-memory 2G --executor-memory 2G \
      --jars target/lib/mongo-hadoop-core-1.3.2.jar,target/lib/mongo-java-driver-2.12.3.jar \
      --class com.mongodb.workshop.SparkExercise \
      target/spark-1.0-SNAPSHOT.jar \
      hdfs://localhost:9000 mongodb://127.0.0.1:27017/movielens predictions
}
add git maven libgfortran3 gcc make 

killall java
ssh_config
mongodb
jdk
install
install_hadoop
install_pig
install_hive
install_spark
install_workshop

#run_examples