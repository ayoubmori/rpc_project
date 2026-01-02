# diag.py
from db_manager import SchoolDB
db = SchoolDB()
db.connect()
cursor = db.conn.cursor()
cursor.execute("SELECT UserID, Email, MotDePasse FROM Utilisateur WHERE Email = 'rachid@school.com'")
row = cursor.fetchone()

if row:
    print(f"Email Found: '{row.Email}'")
    print(f"Hash in DB:  '{row.MotDePasse}'")
    print(f"Hash Length: {len(str(row.MotDePasse))}")
    
    target_hash = "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"
    if str(row.MotDePasse).strip() == target_hash:
        print("✅ SUCCESS: The stripped hash matches '123456'.")
    else:
        print("❌ ERROR: The hashes do NOT match even after stripping.")
else:
    print("❌ ERROR: Email 'rachid@school.com' not found in table.")