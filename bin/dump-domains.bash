#!/bin/bash

today=`date +%Y-%m-%d.%H:%M:%S`

for domain in `cat $1`; do {
  java -cp ./balboa-admin/target/scala-2.10/balboa-admin-assembly-0.16.10-SNAPSHOT.jar com.socrata.balboa.admin.BalboaAdmin dump-only $domain > dumps/$2/domain-$domain-dump-$today-$2.csv &
} done;
