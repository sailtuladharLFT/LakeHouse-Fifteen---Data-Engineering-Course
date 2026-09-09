# Seperate model definition from rest of the project

from dataclasses import dataclass
from typing import Dict, List , Literal, Optional


@dataclass
class CharacterSchema:
    id: int
    name: str
    status: Literal["Alive", "Dead", "unknown"]
    species: str
    type: str
    gender: Literal["Female", "Male", "Genderless", "unknown"]
    origin: Dict[str, str]
    location: Dict[str, str]
    image: str
    episode: List[str]
    url: str
    created: str

@dataclass
class ApiInfo:
    count:int
    pages:int
    next: Optional[str]
    prev: Optional[str]


@dataclass
class ApiResponse:
    info: ApiInfo
    results: List[CharacterSchema]

    def __post_init__(self):
        self.info = ApiInfo(**self.info)
        self.results = [CharacterSchema(**x) for x in self.results]

@dataclass
class ApiParameters:
    page: Optional[int] = None
    name: Optional[str] = None
    status: Optional[Literal["alive" , "dead" , "unknown"]] = None
    species: Optional[str] = None
    type: Optional[str] = None
    gender: Optional[Literal["Female" , "Male" , "Genderless" , "unknown"]] = None


# ---------------------------------------------------------------------------
# DummyJSON Products API  —  https://dummyjson.com/products
# ---------------------------------------------------------------------------


@dataclass
class ProductsPage:
    """One paginated response from the /products endpoint."""
    products: List[Dict]   # raw dicts — parsed into Product only when needed
    total: int
    skip: int
    limit: int

@dataclass
class ProductsPageParams:
    skip: Optional[int] = 0
    # PAGE SIZE = 30
    limit: Optional[int] = 30 


