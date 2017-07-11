from squark.table import get_table


def test_HdfsTable(env):
    table = get_table("hdfs+orc:///test/table", save_format="orc")
    # x = table.env.spark_context
    # import pdb; pdb.set_trace()
