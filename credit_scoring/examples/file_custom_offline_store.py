import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple, Union

import dask.dataframe as dd
import pandas as pd
import pyarrow
import pyarrow.dataset
import pyarrow.parquet
import pytz
from pydantic.typing import Literal

from feast.data_source import DataSource
from feast.errors import (
    FeastJoinKeysDuringMaterialization,
    SavedDatasetLocationAlreadyExists
)
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView

from feast.infra.offline_stores.file_source import (
    FileLoggingDestination,
    FileSource,
    SavedDatasetFileStorage
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob, 
    RetrievalMetadata
)

from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    get_pyarrow_schema_from_batch_source
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage

from feast.usage import log_exceptions_and_usage
from feast.utils import (
    _get_requested_feature_views_to_features_dict,
    _run_dask_field_mapping
)

class FileRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None
    ) -> None:
        """ Initialize a lazy historical retrieval job """
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @log_exceptions_and_usage
    def _to_df_internal(
        self,
        timeout: Optional[int] = None
    ) -> pd.DataFrame:
        df = self.evaluation_function().compute()
        df = df.reset_index(drop=True)
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self) -> pyarrow.Table:
        df = self.evaluation_function().compute()
        return pyarrow.Table.from_pandas(df)

    def persist(
        self, 
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None
    ):
        assert isinstance(storage, SavedDatasetFileStorage)

        if not allow_overwrite and os.path.exists(storage.file_options.uri):
            raise SavedDatasetLocationAlreadyExists(
                location=storage.file_options.uri
            )
        filesystem, path = FileSource.create_filesystem_and_path(
            storage.file_options.uri,
            storage.file_options.s3_endpoint_override,
        )
        if path.endswith('.parquet'):
            pyarrow.parquet.write_table(
                self.to_arrow(),
                where=path,
                filesystem=filesystem
            )
        else:
            pyarrow.parquet.write_to_dataset(
                self.to_arrow(),
                root_path=path,
                filesystem=filesystem
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def supports_remote_storage_export(self) -> bool:
        return False



    

