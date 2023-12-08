from dbt.adapters.base import AdapterPlugin

from dbt.adapters.extrica.column import ExtricaColumn  # noqa
from dbt.adapters.extrica.connections import ExtricaConnectionManager  # noqa
from dbt.adapters.extrica.connections import ExtricaCredentialsFactory
from dbt.adapters.extrica.relation import ExtricaRelation  # noqa

from dbt.adapters.extrica.impl import ExtricaAdapter  # isort: split
from dbt.include import extrica

Plugin = AdapterPlugin(
    adapter=ExtricaAdapter,  # type: ignore
    credentials=ExtricaCredentialsFactory,  # type: ignore
    include_path=extrica.PACKAGE_PATH,
)
