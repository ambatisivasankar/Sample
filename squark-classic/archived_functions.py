# NOTE: From all_tables.py


# NOTE: Function usused in all places
# TODO: Remove unused function
# def push_graphite_stats(stats, table_name):
#     """
#     Function to push data stats (such as count) to graphite.
#     Takes only the stats dictionary as input, but expects to
#     find the 'hostedgraphite' url, token, and port in the
#     secrets file.
#     """
#     stats_time = int(time.time())
#     message = "{API_TOKEN}.squark.{SQUARK_TYPE}.{PROJECT_ID}.{TABLE_NAME}.hdfs_rows {ROWS} {TIME}\n".format(
#         API_TOKEN=GRAPHITE_TOKEN,
#         SQUARK_TYPE=SQUARK_TYPE,
#         PROJECT_ID=PROJECT_ID,
#         TABLE_NAME=table_name,
#         ROWS=stats["count"],
#         TIME=stats_time,
#     )
#     print("SENDING MESSAGE TO GRAPHITE:")
#     print(message.replace(GRAPHITE_TOKEN, "*******"))
#
#     conn = socket.create_connection((GRAPHITE_URL, GRAPHITE_PORT))
#     conn.send(message.encode("utf8"))
#
#     # Send error
#     message = "{API_TOKEN}.squark.{SQUARK_TYPE}.{PROJECT_ID}.{TABLE_NAME}.error_code {ERROR_CODE} {TIME}\n".format(
#         API_TOKEN=GRAPHITE_TOKEN,
#         SQUARK_TYPE=SQUARK_TYPE,
#         PROJECT_ID=PROJECT_ID,
#         TABLE_NAME=table_name,
#         ERROR_CODE=stats["error_code"],
#         TIME=stats_time,
#     )
#     print(message.replace(GRAPHITE_TOKEN, "*******"))
#     conn.send(message.encode("utf8"))
#     conn.close()


# NOTE: unresolved reference dcstats, and this function unused in all places.
# TODO: Remove unused function
# def push_data_catalog_stats(stats, DOMAIN, TOKEN, SCHEMA, TABLE):
#     """
#     This function is going to be used to send stats to the datacatalog api.
#     It requires that an API Token be set  - as well as the domain name.
#     """
#     # This value will be used for every column
#     count = stats.get("count", 0)
#
#     for COLUMN, col_stats in stats["fields"].items():
#         print(
#             "------SENDING STATS FOR SCHEMA: {SCHEMA}  TABLE: {TABLE}  COLUMN: {COLUMN}".format(
#                 SCHEMA=SCHEMA, TABLE=TABLE, COLUMN=COLUMN
#             )
#         )
#
#         # Example stats
#         # {'fields': defaultdict(<class 'dict'>, {'f2': {'countDistinct': 3, 'max': 3, 'min': 1}, '_advana_id': {'max': 7, 'min': 0, 'mean': 3.5, 'countDistinct': 8}, 'f1': {'max': 88, 'min': 11, 'mean': 49.5, 'countDistinct': 8}, '_advana_md5': {'min': 32, 'max': 32, 'countDistinct': 1}}), 'count': 8}
#         count_empty = col_stats.get("count_null", 0)
#         count_unique = col_stats.get("countDistinct", 0)
#         value_min = col_stats.get("min", 0)
#         value_max = col_stats.get("max", 0)
#         value_median = col_stats.get("median", 0)
#         value_mean = col_stats.get("mean", 0)
#         percentile_75 = col_stats.get("percentile_75", 0)
#         percentile_25 = col_stats.get("percentile_25", 0)
#         details = col_stats.get("details", "{}")
#
#         # Send the stats
#         # NOTE: Had to use the .callback function due to the send_stats being decorated by click command.
#         dcstats.send_stats.callback(
#             schema=SCHEMA,
#             table=TABLE,
#             column=COLUMN,
#             count=count,
#             count_empty=count_empty,
#             count_unique=count_unique,
#             value_min=value_min,
#             value_max=value_max,
#             value_median=value_median,
#             value_mean=value_mean,
#             percentile_25=percentile_25,
#             percentile_75=percentile_75,
#             details=details,
#             token=TOKEN,
#             domain=DOMAIN,
#         )
