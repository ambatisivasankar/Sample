'''Generate a dataframe with most dataframe column types represented.
'''
import os
import datetime as dt
from decimal import Decimal, ROUND_DOWN

from pyspark.sql.types import *

from squark.config.environment import Environment


def main():
    env = Environment()

    schema = StructType([
        StructField('string', StringType(), True),
        StructField('binary', BinaryType(), True),
        StructField('boolean', BooleanType(), True),
        StructField('date', DateType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('decimal', DecimalType(), True),
        StructField('double', DoubleType(), True),
        StructField('float', FloatType(), True),
        StructField('byte', ByteType(), True),
        StructField('integer', IntegerType(), True),
        StructField('long', LongType(), True),
        StructField('short', ShortType(), True),
        StructField('array', ArrayType(elementType=IntegerType()), True),
        StructField('map', MapType(keyType=IntegerType(), valueType=IntegerType()), True),
        ])

    values = []
    for n in range(1000):
        val = (
            str(n),
            bin(n),
            bool(n),
            dt.datetime.now().date(),
            dt.datetime.now(),
            Decimal(n * 0.1).quantize(Decimal('.0001'), rounding=ROUND_DOWN),
            n,
            n / 3.3,
            bytes(chr(n % 128), 'ascii'),
            n,
            n,
            n,
            [n],
            {n: n * 33, 21: 4},
            )
        values.append(val)

    rdd = env.spark.ctx.parallelize(values)
    df = env.spark.sqlctx.createDataFrame(rdd, schema)

    try:
        os.makedirs(env.FIXTURES)
    except FileExistsError:
        pass
    df.write.save(os.path.join(env.FIXTURES, 'df1'), mode='overwrite')


if __name__ == '__main__':
    main()
