#!/bin/bash
targetDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
targetDir=$(cd $targetDir/../target 2> /dev/null; pwd)
jar=$(ls -1 $targetDir/mysql-sync-*.jar 2> /dev/null |head -n1)
if [ -z "$jar" ]; then
  echo "No JAR file found, you may need to build first"
  exit 1
fi
scriptDir=$targetDir/sync
mkdir -p $scriptDir 2> /dev/null

SYNCS=""
unset INCREMENTAL
if [ -e "$@" ]; then
  HELP=true
fi
for i in "$@"
do
case $i in
    -s=*|--source=*)
    SOURCE="${i#*=}"
    SOURCE_HOST=$(echo $SOURCE | cut -d ":" -f 1)
    SOURCE_PORT=$(echo $SOURCE | cut -d ":" -f 2)
    if [ -e $SOURCE_PORT ]; then
      SOURCE_PORT=3306
    fi
    shift # past argument=value
    ;;
    -sc=*|--source-credentials=*)
    SOURCE_CREDENTIALS="${i#*=}"
    SOURCE_USER=$(echo $SOURCE_CREDENTIALS | cut -d ":" -f 1)
    SOURCE_PASSWORD=$(echo $SOURCE_CREDENTIALS | cut -d ":" -f 2)
    shift # past argument=value
    ;;
    -t=*|--target=*)
    TARGET="${i#*=}"
    TARGET_HOST=$(echo $TARGET | cut -d ":" -f 1)
    TARGET_PORT=$(echo $TARGET | cut -d ":" -f 2)
    if [ -e $TARGET_PORT ]; then
      TARGET_PORT=3306
    fi
    shift # past argument=value
    ;;
    -tc=*|--target-credentials=*)
    TARGET_CREDENTIALS="${i#*=}"
    TARGET_USER=$(echo $TARGET_CREDENTIALS | cut -d ":" -f 1)
    TARGET_PASSWORD=$(echo $TARGET_CREDENTIALS | cut -d ":" -f 2)
    shift # past argument=value
    ;;
    -i|--incremental)
    INCREMENTAL=true
    shift # past argument=value
    ;;
    -sy=*|--sync=*)
    echo "${i#*=}"
    SYNCS="$SYNCS${i#*=} "
    shift # past argument=value
    ;;
    -a|--anonymize)
    ANONYMIZE=true
    shift # past argument=value
    ;;
    -?|--help|-h)
    HELP=true
    ;;
    *)
    echo "Unknown option ${i}" 1>&2
    missing_param=true
    # unknown option
    ;;
esac
done

if [ -e "$SOURCE_HOST" ]; then
  echo "Missing source host" 1>&2
  missing_param=true
fi
if [ -e "$SOURCE_USER" ]; then
  echo "Missing source user" 1>&2
  missing_param=true
fi
if [ -e "$SOURCE_PASSWORD" ]; then
  echo "Missing source password" 1>&2
  missing_param=true
fi
if [ -e "$TARGET_HOST" ]; then
  echo "Missing target host" 1>&2
  missing_param=true
fi
if [ -e "$TARGET_USER" ]; then
  echo "Missing target user" 1>&2
  missing_param=true
fi
if [ -e "$TARGET_PASSWORD" ]; then
  echo "Missing target password" 1>&2
  missing_param=true
fi
if [ -e "$SYNCS" ]; then
  echo "Missing syncs" 1>&2
  missing_param=true
fi
if [ "$missing_param" == "true" ] || [ "$HELP" == "true" ]; then
  echo "Usage:"
  echo " $0 -a[nonymize] -s[ource]=source_url[:source_port] -s[ource-]c[redentials]=source_user:source_password -t[arget]=target_url[:target_port] -t[arget-]c[redentials]=target_user:target_password [-i[ncremental]] -sy[nc]=source_schema1:target_schema1 [-sy[nc]=source_schema2:target_schema2]"
  echo "Example:"
  echo " $0 -s=localhost:13306 -t=localhost:3316 -sc=sourc_user:xxx -tc=root:mysqlroot -sy=some_other_schema:target"
  if [ "$missing_param" == "true" ]; then
    exit 1
  else
    exit 0
  fi
fi

for s in $SYNCS; do
  SOURCE_SCHEMA=$(echo $s | cut -d ":" -f 1)
  TARGET_SCHEMA=$(echo $s | cut -d ":" -f 2)
  echo "$SOURCE_SCHEMA -> $TARGET_SCHEMA"
  PARAMS="-host=$SOURCE_HOST -target-host=$TARGET_HOST -port=$SOURCE_PORT -target-port=$TARGET_PORT -user=$SOURCE_USER -password=$SOURCE_PASSWORD -target-user=$TARGET_USER -target-password=$TARGET_PASSWORD -source=$SOURCE_SCHEMA -target=$TARGET_SCHEMA -output-file=$scriptDir"
  if [ $INCREMENTAL ]; then
    PARAMS="$PARAMS -incremental"
  fi
  if [ $ANONYMIZE ]; then
    PARAMS="$PARAMS -anonymize"
  fi
  java -Xmx4G -jar $jar $PARAMS || exit 1
done
