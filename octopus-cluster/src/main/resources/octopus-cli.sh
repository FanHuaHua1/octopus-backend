#!/bin/bash

function print_usage(){
  echo "Usage: octopus-cli [start | stop | restart]"
}

function getRunningPID(){
    P_ID=`ps -ef|grep -w $app | grep -v "grep" | awk '{print $2}'`
    if [ $P_ID ]; then
        echo $P_ID
    fi  
}

function start()
{
    echo "Start to check whether the octupus is running"
    P_ID=`getRunningPID`
    if [ $P_ID ]; then
        echo "Octopus is running, please stop it first"
        exit 1
    else
        echo "Octopus is not running, start it now"
        java -jar $app --spring.config.location=conf/application.yaml
        #nohup java -jar $app  &
    fi
}

function stop()
{
    echo "Start to check whether the octupus is running"
    P_ID=`getRunningPID`
    if [ $P_ID ]; then
        kill -9 $P_ID
        echo "Octopus stop"
    else
        echo "Octopus is not running"
    fi
}

function restart()
{
    stop
    sleep 2
    start
}

COMMAND=$1
export HADOOP_USER_NAME=hdfs
export HADOOP_CONF_DIR=/etc/hadoop/conf
export app=octopus-cluster-1.0-SNAPSHOT.jar
case $COMMAND in
  start|stop|restart)
    $COMMAND $SERVER_NAME
    ;;
  *)
    print_usage
    exit 2
    ;;
esac

