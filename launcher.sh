#!/bin/sh
#
# elephantdb init.d script.
#
# All java services require the same directory structure:
#   /usr/local/$APP_NAME
#   /var/log/$APP_NAME
#   /var/run/$APP_NAME

APP_NAME="elephantdb"
APP_USER="backtype"
APP_HOME="/usr/local/$APP_NAME"
JAR_NAME="$APP_NAME-server.jar"

# Finish this up.
APP_OPTS="/build these dynamically!"

DAEMON="/usr/local/bin/daemon"

HEAP_OPTS="-Xmx4096m -Xms4096m -XX:NewSize=768m"
GC_OPTS="-XX:+UseParallelOldGC -XX:+UseAdaptiveSizePolicy -XX:MaxGCPauseMillis=1000 -XX:GCTimeRatio=99"
GC_LOG_OPTS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC"
GC_LOG="-Xloggc:/var/log/$APP_NAME/gc.log"
DEBUG_OPTS="-XX:ErrorFile=/var/log/$APP_NAME/java_error%p.log"
JAVA_OPTS="-server -Djava.library.path=/usr/local/lib $GC_OPTS $GC_LOG_OPTS $GC_LOG $HEAP_OPTS $DEBUG_OPTS"

pidfile="/var/run/$APP_NAME/$APP_NAME.pid"
daemon_pidfile="/var/run/$APP_NAME/$APP_NAME-daemon.pid"
daemon_args="--name $APP_NAME --user $APP_USER --pidfile $daemon_pidfile --core --chdir /"
daemon_start_args="--stdout=/var/log/$APP_NAME/stdout --stderr=/var/log/$APP_NAME/stderr"
ROTATE_GC_CONF="${APP_HOME}/config/rotate-gc.conf"

function running() {
    $DAEMON $daemon_args --running
}

function find_java() {
    if [ ! -z "$JAVA_HOME" ]; then
        return
    fi
    for dir in /opt/jdk /System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home /usr/java/default; do
        if [ -x $dir/bin/java ]; then
            JAVA_HOME=$dir
            break
        fi
    done
}

find_java

function rotate_logs() {
    if [ -f $ROTATE_GC_CONF ]; then
        if [ "$USER" == "root" ]; then
            /usr/sbin/logrotate -f $ROTATE_GC_CONF
        else
            echo "(skipped log rotation, not root)"
        fi
    else
        echo "(skipped log rotation, config doesn't exist)"
    fi
}


case "$1" in
    start)
        rotate_logs
        echo -n "Starting $APP_NAME... "

        if [ ! -r $APP_HOME/$JAR_NAME ]; then
            echo "FAIL"
            echo "*** $APP_NAME jar missing: $APP_HOME/$JAR_NAME - not starting"
            exit 1
        fi
        if [ ! -x $JAVA_HOME/bin/java ]; then
            echo "FAIL"
            echo "*** $JAVA_HOME/bin/java doesn't exist -- check JAVA_HOME?"
            exit 1
        fi
        if running; then
            echo "already running."
            exit 0
        fi

        ulimit -c unlimited || echo -n " (no coredump)"
        $DAEMON $daemon_args $daemon_start_args -- sh -c "echo "'$$'" > $pidfile; exec ${JAVA_HOME}/bin/java ${JAVA_OPTS} -jar ${APP_HOME}/${JAR_NAME} ${APP_OPTS}"
        tries=0
        while ! running; do
            tries=$((tries + 1))
            if [ $tries -ge 5 ]; then
                echo "FAIL"
                exit 1
            fi
            sleep 1
        done
        echo "done."
        ;;
    
    status)
        if running; then
            echo "$APP_NAME is running."
        else
            echo "$APP_NAME is NOT running."
        fi
        ;;
    *)
        echo "Usage: /etc/init.d/${APP_NAME}.sh {start|status}"
        exit 1
        ;;
esac

exit 0
