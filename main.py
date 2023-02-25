from pydantic import BaseModel, Field
from bson import ObjectId, json_util
from typing import List
from datetime import date, datetime
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pymongo import MongoClient
from fastapi.middleware.cors import CORSMiddleware
import ujson
import json


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


class Author(BaseModel):
    id: str = Field(default_factory=PyObjectId, alias="_id")
    name: str

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {"example": {"name": "Diego"}}


class Content(BaseModel):
    id: str = Field(default_factory=PyObjectId, alias="_id")
    id_post: str
    content: str
    images: List[str]
    keyword: List[str]

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "id_post": "id_del_post",
                "content": "Para crear un post con PyMongo con una estructura que incluya título, autor, imágenes, fechas y palabras clave, puedes seguir los siguientes pasos",
                "images": ["Hola.jpg", "Buenas.jpg"],
                "keyword": ["crear", "post"],
            }
        }


class Post(BaseModel):
    id: str = Field(default_factory=PyObjectId, alias="_id")
    title: str
    id_author: str
    images: List[str]
    date = datetime.now()
    categories: List[str]

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        schema_extra = {
            "example": {
                "title": "Creando Post",
                "id_author": "",
                "images": ["Murmullo.jpg"],
                "categories": ["explicacion", "post"],
            }
        }


client = MongoClient()
db_post = client.Sys_Co


def add_post(model: Post):
    try:
        data = db_post.post.insert_one(model)
        if data:
            new_data = db_post.post.find_one({"_id": data.inserted_id})
            return new_data
    except Exception as e:
        print(e)


def retrive_posts():
    try:
        data = [x for x in db_post.post.find({})]
        return data
    except Exception as e:
        print(e)


def add_author(model: Author):
    try:
        data = db_post.author.insert_one(model)
        if data:
            new_data = db_post.author.find_one({"_id": data.inserted_id})
            return new_data

    except Exception as e:
        print(e)


def retrive_authors():
    try:
        data = [x for x in db_post.author.find({})]
        return data
    except Exception as e:
        print(e)


def add_content(model: Content):
    try:
        data = db_post.content.insert_one(model)
        if data:
            new_data = db_post.content.find_one({"_id": data.inserted_id})
            return new_data
    except Exception as e:
        print(e)


def retrive_contents():
    try:
        data = [x for x in db_post.content.find({})]
        return data
    except Exception as e:
        print(e)


def retrive_posts_of_author(id: str):
    try:
        result = db_post.author.aggregate(
            [
                {
                    "$lookup": {
                        "from": "post",
                        "localField": id,
                        "foreignField": id,
                        "as": "posts",
                    }
                },
                {"$project": {"_id": 0}},
            ]
        )
        return ujson.loads(ujson.dumps({"result": list(result)}))
    except Exception as e:
        print(e)


def retrive_post_content(id: str):
    try:
        result = db_post.post.aggregate(
            [
                {"$match": {"_id": id}},
                {
                    "$lookup": {
                        "from": "content",
                        "localField": id,
                        "foreignField": id,
                        "as": "Contenido_Post",
                    }
                },
            ]
        )
        return list(result)
    except Exception as e:
        print(e)


def retrieves_all_posts_with_their_contents():
    try:
        result = db_post.post.aggregate(
            [
                {
                    "$lookup": {
                        "from": "content",
                        "localField": "_id",
                        "foreignField": "id_post",
                        "as": "Contenido_Post",
                    }
                },
            ]
        )
        return list(result)
    except Exception as e:
        print(e)


def retrive_auth_posts(id: str):
    try:
        result = db_post.author.aggregate(
            [
                {"$match": {"_id": id}},
                {
                    "$lookup": {
                        "from": "post",
                        "localField": "_id",
                        "foreignField": "id_author",
                        "as": "Posts",
                    }
                },
                {
                    "$lookup": {
                        "from": "content",
                        "localField": "_id",
                        "foreignField": "id_post",
                        "as": "Content",
                    }
                },
            ]
        )
        return list(result)
    except Exception as e:
        print(e)


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/create/author")
def create_author(author: Author):
    try:
        data = add_author(jsonable_encoder(author))
        return JSONResponse(status_code=200, content={"data": data})

    except Exception as e:
        print(e)


@app.post("/create/post")
def create_post(post: Post):
    try:
        data = add_post(jsonable_encoder(post))
        return JSONResponse(status_code=200, content={"data": data})
    except Exception as e:
        print(e)


@app.post("/create/content")
def create_content(post_content: Content):
    try:
        data = add_content(jsonable_encoder(post_content))
        return JSONResponse(status_code=200, content={"data": data})
    except Exception as e:
        print(e)


@app.get("/retrive/authors")
def get_authors():
    try:
        data = retrive_authors()
        return data
    except Exception as e:
        print(e)


@app.get("/retrive/posts")
def get_posts():
    try:
        data = retrive_posts()
        return data
    except Exception as e:
        print(e)


@app.get("/retrive/contents")
def get_contents():
    try:
        data = retrive_contents()
        return data
    except Exception as e:
        print(e)


@app.get("/retrive/author_posts/")
def get_post_author(id: str):
    try:
        result = retrive_posts_of_author(id)
        return result
    except Exception as e:
        print(e)


@app.get("/retrive/post_content")
def post_content(id: str):
    try:
        data = retrive_post_content(id)
        return data
    except Exception as e:
        print(e)


@app.get("/retrive/posts_and_their_contents")
def retrive_full_posts_and_contents():
    try:
        data = retrieves_all_posts_with_their_contents()
        return data
    except Exception as e:
        print(e)


@app.get('/retrive/obtener_post_con_autho')
def retrive_full(id:str):
    try:
      retulst = retrive_auth_posts(id)
      return retulst
    except Exception as e:
      print(e)