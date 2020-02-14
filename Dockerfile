FROM centos:latest
RUN yum install -y java-1.8.0-openjdk-devel
RUN adduser kafka
RUN chmod -R a+rwx /home/kafka
USER kafka
ADD target/streamproto-0.1-jar-with-dependencies.jar /home/kafka
ENTRYPOINT cd /home/kafka && java -jar streamproto-0.1-jar-with-dependencies.jar

