export PROJECT_ID=verint
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_TABLES="QUEUE,QUEUEHISTORYTIMESERIES,SPQUEUE,SPQUEUESKILL,SKILL,FORECASTTIMESERIES"
export CONNECTION_ID=verint
export SQUARK_METADATA=1
# because of the subsetting below, skipping source row counts entirely
export SKIP_SOURCE_ROW_COUNT=1
# 20171213, FORECASTTIMESERIES has 493 columns, md5 calc is failing in AWS/EMR if > 430 columns are used. Cconfirmed that
#   including the first few columns alone will guarantee uniqueness, below will result in first 400 columns being used
export WIDE_COLUMNS_MD5=1

export JSON_INFO="
{
	'SAVE_TABLE_SQL_SUBQUERY':{
      'schema': 'dbo',
      'table_queries': {
         'QUEUEHISTORYTIMESERIES': '(SELECT * FROM dbo.QUEUEHISTORYTIMESERIES WHERE QUEUEID IN ('''000E3D47.000001''', '''000E525D.000001''', '''00186F0A.000153''', '''001B2BCE.001068''', '''0017072E.000291''','''00139539.000001''', '''0018BFC1.001435''', '''000E3D51.000001''', '''001F3AB7.001903''', '''001AC93C.005173''','''001F3AB7.001904''', '''001F3AB7.001905''', '''000E3D4E.000001''', '''QUEUE2_ID      ''', '''0016C51B.000079''', '''001B2BCE.001070''', '''00124B9A.000001''', '''001857E4.002689''', '''00105431.000001''', '''001EFB2D.003132''','''001D9413.006918''','''001857E4.002690''','''000E93D8.000001''','''001B2BCE.001663''','''QUEUE1_ID''') AND TIME >= '''2013-01-01 00:00:00''') as subquery'}
 	}
}"



