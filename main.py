from pymongo import MongoClient
from bson import ObjectId
from typing import Optional, List


class MongoTaskRepository:
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def create_task(self, data: dict) -> str:
        result = self.collection.insert_one(data)
        return str(result.inserted_id)

    def get_task_by_id(self, task_id: str) -> Optional[dict]:
        task = self.collection.find_one({'_id': ObjectId(task_id)})
        return task

    def delete_task(self, task_id: str) -> bool:
        result = self.collection.delete_one({'_id': ObjectId(task_id)})
        return result.deleted_count > 0

    def aggregate_by_tags(self) -> List[dict]:
        pipeline = [
            {'$unwind': '$tags'},
            {'$group': {
                '_id': '$tags',
                'count': {'$sum': 1}
            }},
            {'$project': {
                'tag': '$_id',
                'count': 1,
                '_id': 0
            }}
        ]
        result = list(self.collection.aggregate(pipeline))
        return result


if __name__ == '__main__':
    mongo_url = 'mongodb://localhost:27017/'
    db_name = 'task_db'
    collection_name = 'tasks'

    repo = MongoTaskRepository(mongo_url, db_name, collection_name)

    # Создание задачи
    task_data = {
        'title': 'Задача 1',
        'tags': ['work', 'urgent'],
        'owner': 'user1',
        'custom_fields': {'priority': 'high'}
    }
    task_id = repo.create_task(task_data)
    print(f'Создана задача с ID: {task_id}')

    # Получение задачи по ID
    task = repo.get_task_by_id(task_id)
    print(f'Получена задача: {task}')

    # Агрегация по тегам
    tags_count = repo.aggregate_by_tags()
    print(f'Количество задач по тегам: {tags_count}')

    # Удаление задачи
    deleted = repo.delete_task(task_id)
    print(f'Задача удалена: {deleted}')
