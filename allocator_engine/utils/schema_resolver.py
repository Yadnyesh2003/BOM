import polars as pl

class SchemaResolver:

    @staticmethod
    def resolve(
        df: pl.DataFrame,
        schema_cfg: dict,
        required_keys: list,
        df_name: str,
        logger
    ) -> pl.DataFrame:
        """
        - Validates required columns
        - Renames to semantic names
        - Drops extra columns
        """

        missing = [k for k in required_keys if k not in schema_cfg]
        if missing:
            logger.error(
                f"Schema config missing keys for {df_name}: {missing}"
            )
            raise ValueError("Invalid schema configuration")

        rename_map = {}
        for key in required_keys:
            actual_col = schema_cfg[key]
            if actual_col not in df.columns:
                logger.error(
                    f"Missing column '{actual_col}' in {df_name} dataframe"
                )
                raise ValueError("Input file schema mismatch")
            if df[actual_col].null_count() == df.height:
                logger.warning(
                    f"Column '{actual_col}' in {df_name} is completely empty"
                )
            rename_map[actual_col] = key

        df = df.rename(rename_map)

        # Drop unwanted columns
        df = df.select(required_keys)

        logger.debug(
            f"{df_name} schema resolved. Columns: {df.columns}"
        )

        return df
