from pyspark.sql import functions as F


class SensorTransformer:
    SENSOR_COLUMNS = [
        "T_xacc", "T_yacc", "T_zacc", "T_xgyro", "T_ygyro", "T_zgyro", "T_xmag", "T_ymag", "T_zmag",
        "RA_xacc", "RA_yacc", "RA_zacc", "RA_xgyro", "RA_ygyro", "RA_zgyro", "RA_xmag", "RA_ymag", "RA_zmag",
        "LA_xacc", "LA_yacc", "LA_zacc", "LA_xgyro", "LA_ygyro", "LA_zgyro", "LA_xmag", "LA_ymag", "LA_zmag",
        "RL_xacc", "RL_yacc", "RL_zacc", "RL_xgyro", "RL_ygyro", "RL_zgyro", "RL_xmag", "RL_ymag", "RL_zmag",
        "LL_xacc", "LL_yacc", "LL_zacc", "LL_xgyro", "LL_ygyro", "LL_zgyro", "LL_xmag", "LL_ymag", "LL_zmag",
    ]

    BODY_PARTS = {
        "T": ("T_xacc", "T_yacc", "T_zacc", "T_xgyro", "T_ygyro", "T_zgyro", "T_xmag", "T_ymag", "T_zmag"),
        "RA": ("RA_xacc", "RA_yacc", "RA_zacc", "RA_xgyro", "RA_ygyro", "RA_zgyro", "RA_xmag", "RA_ymag", "RA_zmag"),
        "LA": ("LA_xacc", "LA_yacc", "LA_zacc", "LA_xgyro", "LA_ygyro", "LA_zgyro", "LA_xmag", "LA_ymag", "LA_zmag"),
        "RL": ("RL_xacc", "RL_yacc", "RL_zacc", "RL_xgyro", "RL_ygyro", "RL_zgyro", "RL_xmag", "RL_ymag", "RL_zmag"),
        "LL": ("LL_xacc", "LL_yacc", "LL_zacc", "LL_xgyro", "LL_ygyro", "LL_zgyro", "LL_xmag", "LL_ymag", "LL_zmag"),
    }

    def __init__(self, feature_columns=None):
        self.feature_columns = feature_columns

    def transform(self, df):
        if "sensor_values" in df.columns:
            df = df.select(
                *([F.col("segment_id")] if "segment_id" in df.columns else []),
                *[
                    F.col(f"sensor_values.{column}").cast("double").alias(column)
                    for column in self.SENSOR_COLUMNS
                ]
            )

        if "segment_id" not in df.columns:
            df = df.withColumn("segment_id", F.lit(1))
        df, magnitude_columns = self._add_magnitude_columns(df)
        sensor_columns = self.SENSOR_COLUMNS + magnitude_columns

        feature_df = df.groupBy("segment_id").agg(
            *self._feature_formulas(sensor_columns)
        )

        feature_df = feature_df.drop("segment_id")

        if self.feature_columns:
            feature_df = feature_df.select(*self.feature_columns)

        return feature_df

    def _add_magnitude_columns(self, df):
        magnitude_columns = []

        for part, columns in self.BODY_PARTS.items():
            xacc, yacc, zacc, xgyro, ygyro, zgyro, xmag, ymag, zmag = columns

            acc_mag = f"{part}_acc_mag"
            gyro_mag = f"{part}_gyro_mag"
            mag_mag = f"{part}_mag_mag"

            df = df.withColumn(
                acc_mag,
                F.sqrt(F.col(xacc) ** 2 + F.col(yacc) ** 2 + F.col(zacc) ** 2),
            )
            df = df.withColumn(
                gyro_mag,
                F.sqrt(F.col(xgyro) ** 2 + F.col(ygyro) ** 2 + F.col(zgyro) ** 2),
            )
            df = df.withColumn(
                mag_mag,
                F.sqrt(F.col(xmag) ** 2 + F.col(ymag) ** 2 + F.col(zmag) ** 2),
            )

            magnitude_columns.extend([acc_mag, gyro_mag, mag_mag])

        return df, magnitude_columns

    def _feature_formulas(self, columns):
        formulas = []

        for column in columns:
            value = F.col(column)
            formulas.extend([
                F.mean(value).alias(f"{column}_mean"),
                F.stddev(value).alias(f"{column}_std"),
                F.min(value).alias(f"{column}_min"),
                F.max(value).alias(f"{column}_max"),
                F.skewness(value).alias(f"{column}_skew"),
                F.kurtosis(value).alias(f"{column}_kurtosis"),
            ])

        return formulas
