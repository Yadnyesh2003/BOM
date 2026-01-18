import polars as pl
import re

class SchemaResolver:

    @staticmethod
    def _normalize(col: str) -> str:
        """
        Canonical column representation for comparison:
        - strip leading/trailing spaces
        - lowercase
        - collapse multiple spaces
        """
        col = col.strip().lower()
        col = re.sub(r"\s+", " ", col)
        return col

    @staticmethod
    def resolve(
        df: pl.DataFrame,
        schema_cfg: dict,
        required_keys: list,
        df_name: str,
        logger
    ) -> pl.DataFrame:
        """
        - Validates required columns (case/space insensitive)
        - Renames to semantic names (clean, canonical)
        - Drops extra columns
        """

        # Validate schema config
        missing = [k for k in required_keys if k not in schema_cfg]
        if missing:
            logger.error(
                f"Schema config missing keys for {df_name}: {missing}"
            )
            raise ValueError("Invalid schema configuration")

        # Build normalized lookup of dataframe columns
        df_cols = df.columns
        normalized_df_cols = {
            SchemaResolver._normalize(c): c for c in df_cols
        }

        rename_map = {}

        # Resolve required columns
        for key in required_keys:
            expected_col = schema_cfg[key]
            norm_expected = SchemaResolver._normalize(expected_col)

            if norm_expected not in normalized_df_cols:
                logger.error(
                    f"Missing column '{expected_col}' in {df_name} dataframe "
                    f"(after normalization)"
                )
                raise ValueError("Input file schema mismatch")

            actual_col = normalized_df_cols[norm_expected]

            if df[actual_col].null_count() == df.height:
                logger.warning(
                    f"Column '{actual_col}' in {df_name} is completely empty"
                )

            rename_map[actual_col] = key

        # Rename to clean semantic names
        df = df.rename(rename_map)
        # Drop unwanted columns
        df = df.select(required_keys)
        logger.debug("%s schema resolved. Columns: %s", df_name, df.columns)

        return df