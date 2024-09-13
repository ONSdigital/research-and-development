"""Function to run the mapping of foreign ownership (ultfoc)"""

import logging
import pandas as pd
from typing import Tuple

from src.mapping import mapping_helpers as hlp

MappingLogger = logging.getLogger(__name__)


def validate_ultfoc_mapper(ultfoc_mapper: pd.DataFrame) -> None:
    """
    Validate the foreign ownership (ultfoc) mapper.

    NOTE: we can allow this mapper to contain null values in the ultfoc

    Args:
        ultfoc_mapper (pd.DataFrame): The foreign ownership mapper DataFrame.

    Returns:
        pd.DataFrame: The validated foreign ownership mapper DataFrame.
    """
    hlp.col_validation_checks(ultfoc_mapper, "ultfoc", "ultfoc", str, 2, True)
    hlp.check_mapping_unique(ultfoc_mapper, "ruref")


def join_fgn_ownership(
    responses: Tuple[pd.DataFrame, pd.DataFrame],
    mapper_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Validate and join the foreign ownership (ultfoc) mapper to the responses dataframes.

    Args:
        responses (Tuple[pd.DataFrame, pd.DataFrame]): The GB & NI responses dataframes
        mapper_df (pd.DataFrame): The mapper DataFrame.

    Returns:
        pd.DataFrame: The combined DataFrame resulting from the left join.
    """
    # perform validation on the foreign ownership (ultfoc) mapper
    validate_ultfoc_mapper(mapper_df)

    gb_df, ni_df = responses

    # process NI data if it exists
    if not ni_df.empty:
        mapped_ni_df = ni_df.rename(columns={"foc": "ultfoc"})
        mapped_ni_df["ultfoc"] = mapped_ni_df["ultfoc"].fillna("GB")
        mapped_ni_df["ultfoc"] = mapped_ni_df["ultfoc"].replace("", "GB")
    else:
        mapped_ni_df = ni_df

    # process GB data
    mapped_gb_df = gb_df.merge(
        mapper_df,
        how="left",
        left_on="reference",
        right_on="ruref",
    )

    mapped_gb_df.drop(columns=["ruref"], inplace=True)

    # Report unmapped ultfoc
    # Choose entries with Null or empty string ultfoc
    unmapped_gb_df = mapped_gb_df.loc[
        (mapped_gb_df["ultfoc"].isna()) | (mapped_gb_df["ultfoc"] == "")
    ]

    # Keep the unique references only
    unmapped_refs = unmapped_gb_df[["reference"]].drop_duplicates()
    
    # Calculate the number of unmapped references
    num_unmapped = unmapped_refs.shape[0]

    # If we have some unmapped references, report them and fillna with GB
    if num_unmapped:
        MappingLogger.info(f"Found {num_unmapped} references with blank ultfoc.")
        unmapped_list = unmapped_refs["reference"].tolist()

        # Put all references in the single column, with no prefix
        report = ""
        for ref in unmapped_list:
            report += "\n + str(ref)
        
        MappingLogger.info(f"The following references were unmapped:{report}")
        
        # If there were unmapped ultfoc values, give them GB
        MappingLogger.info("Filling in the unmapped ultfoc with GB")
        mapped_gb_df["ultfoc"] = mapped_gb_df["ultfoc"].fillna("GB")
        mapped_gb_df["ultfoc"] = mapped_gb_df["ultfoc"].replace("", "GB")
    else:
        # If there are no unmapped ultfoc, just report success
        MappingLogger.info(f"All references are mapped to ultfoc.")

    MappingLogger.info("Mapping and validation of ultfoc successfully completed.")
    return mapped_gb_df, mapped_ni_df
