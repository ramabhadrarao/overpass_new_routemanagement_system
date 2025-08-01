from datetime import datetime
from bson import ObjectId
import bcrypt

class User:
    collection_name = 'users'
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        
    def create_user(self, username, email, password, role='user'):
        """Create a new user"""
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        user_doc = {
            'username': username,
            'email': email,
            'password': hashed_password,
            'role': role,
            'is_active': True,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'last_login': None,
            'login_attempts': 0,
            'lock_until': None
        }
        
        return self.collection.insert_one(user_doc)
    
    def find_by_username(self, username):
        """Find user by username"""
        return self.collection.find_one({'username': username})
    
    def find_by_id(self, user_id):
        """Find user by ID"""
        return self.collection.find_one({'_id': ObjectId(user_id)})
    
    def verify_password(self, user, password):
        """Verify user password"""
        if not user:
            return False
        return bcrypt.checkpw(password.encode('utf-8'), user['password'])
    
    def update_last_login(self, user_id):
        """Update user's last login time"""
        return self.collection.update_one(
            {'_id': ObjectId(user_id)},
            {
                '$set': {
                    'last_login': datetime.utcnow(),
                    'login_attempts': 0
                }
            }
        )
    
    def increment_login_attempts(self, username):
        """Increment failed login attempts"""
        return self.collection.update_one(
            {'username': username},
            {'$inc': {'login_attempts': 1}}
        )