#!/bin/bash

HBASE_NAMESPACE='banking_shamithna75gedu'
HBASE_TABLE='suspicious_transaction'
COLUMN_FAMILY='info'

echo "create_namespace '$HBASE_NAMESPACE'" | /opt/hbase/bin/hbase shell
echo "create '$HBASE_NAMESPACE:$HBASE_TABLE', {NAME => '$COLUMN_FAMILY', VERSIONS => 4}" | /opt/hbase/bin/hbase shell