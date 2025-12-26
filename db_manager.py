import pyodbc
import os
import hashlib
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class SchoolDB:
    def __init__(self):
        # 1. Force FreeTDS Driver for Raspberry Pi
        self.driver = os.getenv('DB_DRIVER', '{FreeTDS}')
        self.server = os.getenv('DB_SERVER', '192.168.11.125') 
        self.database = os.getenv('DB_DATABASE', 'SchoolManagementDB')
        self.user = os.getenv('DB_USER', 'ayoub_rpc')
        self.password = os.getenv('DB_PASSWORD', 'ayoub_rpc')
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn: self.conn.close()

    def connect(self):
        try:
            # Added MARS_Connection=yes to allow multiple cursors at once
            conn_str = (
                f"DRIVER={self.driver};SERVER={self.server};PORT=1433;"
                f"DATABASE={self.database};UID={self.user};PWD={self.password};"
                f"TDS_Version=7.4;TrustServerCertificate=yes;"
                f"MARS_Connection=yes;" 
            )
            self.conn = pyodbc.connect(conn_str, timeout=10)
        except Exception as e:
            logger.error(f"❌ Connection Error: {e}")
            self.conn = None

    # --- AUTHENTICATION ---
    def login(self, email, password):
        if not self.conn: return None
        cursor = self.conn.cursor()
        
        # 1. Fetch the user's stored hash
        sql = "SELECT UserID, Nom, Prenom, Role, MotDePasse FROM Utilisateur WHERE Email = ?"
        cursor.execute(sql, (email,))
        row = cursor.fetchone()
        
        if not row: return None

        # 2. Check Password
        # Case A: Input is ALREADY a SHA256 Hash (64 chars long) -> Direct Compare
        if len(password) == 64 and row.MotDePasse == password:
             return {"id": row.UserID, "name": f"{row.Nom} {row.Prenom}", "role": row.Role, "email": email}
             
        # Case B: Input is Plain Text (e.g. "123456") -> Hash it first, then Compare
        input_hash = hashlib.sha256(password.encode()).hexdigest()
        if row.MotDePasse == input_hash:
             return {"id": row.UserID, "name": f"{row.Nom} {row.Prenom}", "role": row.Role, "email": email}

        return None

    # --- ADMIN: USER MANAGEMENT ---
    def get_all_users_extended(self):
        if not self.conn: return []
        cursor = self.conn.cursor()
        sql = """
            SELECT U.UserID, U.Nom, U.Prenom, U.Email, U.Role, G.NomGroupe, F.Matricule, E.CNE
            FROM Utilisateur U
            LEFT JOIN Etudiant E ON U.UserID = E.EtudiantID
            LEFT JOIN Groupe G ON E.GroupeID = G.GroupeID
            LEFT JOIN Formateur F ON U.UserID = F.FormateurID
            ORDER BY U.Role, U.Nom
        """
        cursor.execute(sql)
        users = []
        for r in cursor.fetchall():
            users.append({
                "id": r.UserID, "name": f"{r.Nom} {r.Prenom}", "email": r.Email, 
                "role": r.Role, "student_group": r.NomGroupe, 
                "matricule": r.Matricule, "cne": r.CNE, "teacher_groups": []
            })
        
        # Populate Teacher Groups
        cursor.execute("SELECT A.FormateurID, G.NomGroupe, M.NomModule FROM Affectation A JOIN Groupe G ON A.GroupeID = G.GroupeID JOIN Module M ON A.ModuleID = M.ModuleID")
        for assign in cursor.fetchall():
            for u in users:
                if u['id'] == assign.FormateurID:
                    u['teacher_groups'].append(f"{assign.NomGroupe} ({assign.NomModule})")
        return users

    def get_groups_by_filiere(self):
        if not self.conn: return {}
        cursor = self.conn.cursor()
        cursor.execute("SELECT F.NomFiliere, G.GroupeID, G.NomGroupe FROM Groupe G JOIN Filiere F ON G.FiliereID=F.FiliereID")
        res = {}
        for r in cursor.fetchall():
            if r.NomFiliere not in res: res[r.NomFiliere] = []
            res[r.NomFiliere].append({'id': r.GroupeID, 'name': r.NomGroupe})
        return res

    def get_all_modules(self):
        if not self.conn: return []
        cursor = self.conn.cursor()
        cursor.execute("SELECT ModuleID, NomModule FROM Module")
        return [{"id": r.ModuleID, "name": r.NomModule} for r in cursor.fetchall()]

    def create_user_account(self, nom, prenom, email, password, role, extra):
        if not self.conn: return False
        hashed_pw = hashlib.sha256(password.encode()).hexdigest()
        cursor = self.conn.cursor()
        try:
            cursor.execute("INSERT INTO Utilisateur (Nom, Prenom, Email, MotDePasse, Role) VALUES (?,?,?,?,?)", (nom, prenom, email, hashed_pw, role))
            cursor.execute("SELECT @@IDENTITY"); uid = cursor.fetchone()[0]
            if role == 'Etudiant': cursor.execute("INSERT INTO Etudiant (EtudiantID, CNE, GroupeID, DateNaissance) VALUES (?,?,?,GETDATE())", (uid, extra.get('cne'), extra.get('groupe_id')))
            elif role == 'Formateur': cursor.execute("INSERT INTO Formateur (FormateurID, Matricule, Specialite) VALUES (?,?,'General')", (uid, extra.get('matricule')))
            self.conn.commit(); return True
        except Exception: self.conn.rollback(); return False

    # --- ASSIGNMENTS (Renamed to match app.py) ---
    def assign_formateur_to_module(self, formateur_id, groupe_id, module_id):
        """ Links a teacher to a group and module in the Affectation table """
        if not self.conn: 
            return False
        
        cursor = self.conn.cursor()
        try:
            # Check if assignment already exists to prevent duplicate key errors
            cursor.execute("SELECT 1 FROM Affectation WHERE FormateurID=? AND GroupeID=? AND ModuleID=?", 
                           (formateur_id, groupe_id, module_id))
            if cursor.fetchone(): 
                return False

            sql = "INSERT INTO Affectation (FormateurID, GroupeID, ModuleID) VALUES (?, ?, ?)"
            cursor.execute(sql, (formateur_id, groupe_id, module_id))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"❌ Assignment Error: {e}")
            self.conn.rollback()
            return False

    def get_teacher_assignments_detailed(self, fid):
        cursor = self.conn.cursor()
        cursor.execute("SELECT A.AffectationID, G.NomGroupe, M.NomModule FROM Affectation A JOIN Groupe G ON A.GroupeID=G.GroupeID JOIN Module M ON A.ModuleID=M.ModuleID WHERE A.FormateurID=?", (fid,))
        return [{"id": r.AffectationID, "group": r.NomGroupe, "module": r.NomModule} for r in cursor.fetchall()]

    def delete_assignment(self, aid):
        try:
            self.conn.cursor().execute("DELETE FROM Affectation WHERE AffectationID=?", (aid,))
            self.conn.commit(); return True
        except Exception: return False

    def update_user(self, user_id, data):
        cursor = self.conn.cursor()
        try:
            if data.get('password'):
                pw = hashlib.sha256(data['password'].encode()).hexdigest()
                cursor.execute("UPDATE Utilisateur SET Nom=?, Prenom=?, Email=?, MotDePasse=? WHERE UserID=?", (data['nom'], data['prenom'], data['email'], pw, user_id))
            else:
                cursor.execute("UPDATE Utilisateur SET Nom=?, Prenom=?, Email=? WHERE UserID=?", (data['nom'], data['prenom'], data['email'], user_id))
            if data['role'] == 'Etudiant': cursor.execute("UPDATE Etudiant SET CNE=?, GroupeID=? WHERE EtudiantID=?", (data.get('cne'), data.get('groupe_id'), user_id))
            elif data['role'] == 'Formateur': cursor.execute("UPDATE Formateur SET Matricule=? WHERE FormateurID=?", (data.get('matricule'), user_id))
            self.conn.commit(); return True
        except Exception: self.conn.rollback(); return False

    def delete_user(self, user_id):
        try:
            self.conn.cursor().execute("DELETE FROM Utilisateur WHERE UserID=?", (user_id,))
            self.conn.commit(); return True
        except Exception: return False

    def get_user_details(self, user_id):
        if not self.conn: return None
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM Utilisateur WHERE UserID = ?", (user_id,))
        u = cursor.fetchone()
        if not u: return None
        data = {"id": u.UserID, "nom": u.Nom, "prenom": u.Prenom, "email": u.Email, "role": u.Role, "cne":"", "matricule":"", "groupe_id":""}
        if u.Role == 'Etudiant':
            cursor.execute("SELECT CNE, GroupeID FROM Etudiant WHERE EtudiantID=?", (user_id,))
            ext = cursor.fetchone()
            if ext: data.update({'cne': ext.CNE, 'groupe_id': ext.GroupeID})
        elif u.Role == 'Formateur':
            cursor.execute("SELECT Matricule FROM Formateur WHERE FormateurID=?", (user_id,))
            ext = cursor.fetchone()
            if ext: data.update({'matricule': ext.Matricule})
        return data

    # --- 3. FIX: GLOBAL TPs (Use LEFT JOIN) ---
    # Ensure this def is aligned with other methods like get_absent_report
    def get_all_tps_global(self):
        """ Fetches all TPs for the Admin Global Content tab with high reliability. """
        if not self.conn: 
            return []
        
        with self.conn.cursor() as cursor:
            # Using LEFT JOIN ensures TPs show up even if a Group/Module was deleted
            sql = """
                SELECT TP.TPID, TP.Titre, TP.DateLimite, 
                       ISNULL(G.NomGroupe, 'No Group') as GroupName, 
                       ISNULL(M.NomModule, 'General') as ModuleName, 
                       ISNULL(U.Nom, 'System') as Nom, 
                       ISNULL(U.Prenom, 'Admin') as Prenom
                FROM TP 
                LEFT JOIN Groupe G ON TP.GroupeID = G.GroupeID 
                LEFT JOIN Module M ON TP.ModuleID = M.ModuleID 
                LEFT JOIN Utilisateur U ON TP.FormateurID = U.UserID
                ORDER BY TP.DateLimite DESC
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            return [{
                "id": r.TPID, 
                "titre": r.Titre, 
                "deadline": str(r.DateLimite)[:16] if r.DateLimite else "No Deadline", 
                "group": r.GroupName, 
                "module": r.ModuleName, 
                "teacher": f"{r.Nom} {r.Prenom}"
            } for r in rows]

    def get_presence_stats(self, formateur_id=None):
        if not self.conn: return []
        with self.conn.cursor() as cursor:
            where_sql = "WHERE S.FormateurID = ?" if formateur_id else ""
            sql = f"""
                SELECT CAST(S.DateDebut AS DATE) as Day, 
                       ISNULL(G.NomGroupe, 'N/A') as GroupName, 
                       COUNT(CASE WHEN LOWER(P.Etat) = 'present' THEN 1 END) as Present, 
                       COUNT(P.PresenceID) as Total
                FROM Seance S 
                LEFT JOIN Groupe G ON S.GroupeID = G.GroupeID 
                LEFT JOIN Presence P ON S.SeanceID = P.SeanceID 
                {where_sql} 
                GROUP BY CAST(S.DateDebut AS DATE), G.NomGroupe
            """
            if formateur_id: cursor.execute(sql, (formateur_id,))
            else: cursor.execute(sql)
            
            rows = cursor.fetchall()
            return [{"date": str(r.Day), "group": r.GroupName, "rate": round((r.Present/r.Total*100), 1) if r.Total > 0 else 0} for r in rows]

    def get_absent_report(self, formateur_id=None):
        if not self.conn: return []
        with self.conn.cursor() as cursor:
            where_sql = "AND S.FormateurID = ?" if formateur_id else ""
            sql = f"""
                SELECT U.Nom, U.Prenom, E.CNE, G.NomGroupe, M.NomModule, S.DateDebut 
                FROM Presence P 
                JOIN Seance S ON P.SeanceID = S.SeanceID 
                JOIN Etudiant E ON P.EtudiantID = E.EtudiantID 
                JOIN Utilisateur U ON E.EtudiantID = U.UserID 
                LEFT JOIN Groupe G ON S.GroupeID = G.GroupeID 
                LEFT JOIN Module M ON S.ModuleID = M.ModuleID 
                WHERE LOWER(P.Etat) = 'absent' {where_sql}
            """
            if formateur_id: cursor.execute(sql, (formateur_id,))
            else: cursor.execute(sql)
            
            rows = cursor.fetchall()
            rep = {}
            for r in rows:
                k = f"{r.CNE}-{r.NomModule}"
                if k not in rep: rep[k] = {"name": f"{r.Nom} {r.Prenom}", "cne": r.CNE, "group": r.NomGroupe, "module": r.NomModule, "count": 0, "dates": []}
                rep[k]["count"] += 1; rep[k]["dates"].append(r.DateDebut.strftime("%d %b"))
            return sorted(list(rep.values()), key=lambda x: x['count'], reverse=True)
        
    def get_global_kpis(self, formateur_id=None):
        if not self.conn: return {"total_sessions": 0, "avg_rate": 0}
        cursor = self.conn.cursor()
        where_sql = "WHERE FormateurID = ?" if formateur_id else ""
        
        if formateur_id:
            cursor.execute(f"SELECT COUNT(*) FROM Seance {where_sql}", (formateur_id,))
        else:
            cursor.execute(f"SELECT COUNT(*) FROM Seance {where_sql}")
            
        total = cursor.fetchone()[0]
        return {"total_sessions": total, "avg_rate": 0}

    # --- 5. TPs & BLOBs ---
    def create_tp_with_blob(self, titre, desc, f_bytes, f_name, f_type, deadline, mid, fid, gid):
        cursor = self.conn.cursor()
        try:
            sql = "INSERT INTO TP (Titre, Description, FichierData, FichierNom, FichierType, DateLimite, ModuleID, FormateurID, GroupeID) VALUES (?,?,?,?,?,?,?,?,?)"
            cursor.execute(sql, (titre, desc, pyodbc.Binary(f_bytes), f_name, f_type, deadline.replace('T', ' '), mid, fid, gid))
            self.conn.commit(); return True
        except Exception: return False

    def get_tp_file_content(self, tp_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT FichierData, FichierNom FROM TP WHERE TPID=?", (tp_id,))
        row = cursor.fetchone()
        return {"data": row.FichierData, "name": row.FichierNom} if row else None

    def get_tps_for_student(self, gid):
        cursor = self.conn.cursor()
        cursor.execute("SELECT TPID, Titre, Description, DateLimite, NomModule FROM TP JOIN Module ON TP.ModuleID=Module.ModuleID WHERE GroupeID=? ORDER BY DateLimite DESC", (gid,))
        return [{"id": r.TPID, "titre": r.Titre, "description": r.Description, "deadline": str(r.DateLimite), "module": r.NomModule} for r in cursor.fetchall()]

    def submit_rapport_file(self, tpid, uid, f_bytes, f_name, f_type):
        cursor = self.conn.cursor()
        try:
            sql = "INSERT INTO Soumission (TPID, EtudiantID, FichierData, FichierNom, FichierType, DateSoumission) VALUES (?,?,?,?,?,GETDATE())"
            cursor.execute(sql, (tpid, uid, pyodbc.Binary(f_bytes), f_name, f_type))
            self.conn.commit(); return True
        except Exception: return False

    def get_submissions_for_tp(self, tpid):
        cursor = self.conn.cursor()
        sql = "SELECT S.SoumissionID, U.Nom, U.Prenom, S.DateSoumission, S.Note, S.FichierNom FROM Soumission S JOIN Utilisateur U ON S.EtudiantID=U.UserID WHERE S.TPID=? ORDER BY U.Nom"
        cursor.execute(sql, (tpid,))
        return [{"id": r.SoumissionID, "student": f"{r.Nom} {r.Prenom}", "date": r.DateSoumission.strftime("%d %b %H:%M"), "grade": r.Note if r.Note is not None else "", "file_name": r.FichierNom} for r in cursor.fetchall()]

    def save_grade(self, sid, grade):
        try:
            self.conn.cursor().execute("UPDATE Soumission SET Note=? WHERE SoumissionID=?", (grade, sid))
            self.conn.commit(); return True
        except Exception: return False

    def get_teacher_modules(self, formateur_id):
        """ Fetches the classes a teacher is assigned to """
        if not self.conn: return []
        with self.conn.cursor() as cursor:
            sql = """
                SELECT M.ModuleID, M.NomModule, G.GroupeID, G.NomGroupe 
                FROM Affectation A 
                JOIN Module M ON A.ModuleID = M.ModuleID 
                JOIN Groupe G ON A.GroupeID = G.GroupeID 
                WHERE A.FormateurID = ?
            """
            cursor.execute(sql, (formateur_id,))
            return [{"module_id": r.ModuleID, "module_name": r.NomModule, 
                     "group_id": r.GroupeID, "group_name": r.NomGroupe} for r in cursor.fetchall()]
    
    def get_formateur_history_mixed(self, formateur_id):
        """ Fetches both TPs and Announcements for the teacher's history table """
        if not self.conn: return []
        with self.conn.cursor() as cursor:
            sql = """
                SELECT TPID as ID, Titre, DateLimite as D, 'TP' as T, G.NomGroupe, M.NomModule 
                FROM TP 
                LEFT JOIN Groupe G ON TP.GroupeID = G.GroupeID 
                LEFT JOIN Module M ON TP.ModuleID = M.ModuleID 
                WHERE FormateurID = ?
                UNION ALL
                SELECT AnnonceID, Titre, DatePublication, 'Annonce', G.NomGroupe, M.NomModule 
                FROM Annonce 
                LEFT JOIN Groupe G ON Annonce.GroupeID = G.GroupeID 
                LEFT JOIN Module M ON Annonce.ModuleID = M.ModuleID 
                WHERE FormateurID = ?
                ORDER BY D DESC
            """
            cursor.execute(sql, (formateur_id, formateur_id))
            return [{"id": r.ID, "title": r.Titre, "date": str(r.D)[:16], "type": r.T, 
                     "group": r.NomGroupe or "N/A", "module": r.NomModule or "N/A"} for r in cursor.fetchall()]
    
    def create_annonce(self, titre, contenu, image_bytes, formateur_id, groupe_id, module_id):
        try:
            sql = "INSERT INTO Annonce (Titre, Contenu, ImageBin, FormateurID, GroupeID, ModuleID, DatePublication) VALUES (?,?,?,?,?,?,GETDATE())"
            self.conn.cursor().execute(sql, (titre, contenu, pyodbc.Binary(image_bytes) if image_bytes else None, formateur_id, groupe_id, module_id))
            self.conn.commit(); return True
        except Exception: return False

    def get_or_create_seance(self, fid, gid, mid, date_str):
        cursor = self.conn.cursor()
        cursor.execute("SELECT SeanceID FROM Seance WHERE FormateurID=? AND GroupeID=? AND ModuleID=? AND CAST(DateDebut AS DATE)=?", (fid, gid, mid, date_str))
        row = cursor.fetchone()
        if row: return row[0]
        cursor.execute("INSERT INTO Seance (DateDebut, DateFin, Salle, ModuleID, FormateurID, GroupeID) VALUES (?,?,'Virtual',?,?,?)", (f"{date_str} 08:00:00", f"{date_str} 10:00:00", mid, fid, gid))
        self.conn.commit(); cursor.execute("SELECT @@IDENTITY"); return cursor.fetchone()[0]

    def get_students_with_presence(self, gid, sid):
        cursor = self.conn.cursor()
        sql = "SELECT E.EtudiantID, U.Nom, U.Prenom, E.CNE, P.Etat FROM Etudiant E JOIN Utilisateur U ON E.EtudiantID=U.UserID LEFT JOIN Presence P ON E.EtudiantID=P.EtudiantID AND P.SeanceID=? WHERE E.GroupeID=? ORDER BY U.Nom"
        cursor.execute(sql, (sid, gid))
        return [{"id": r.EtudiantID, "name": f"{r.Nom} {r.Prenom}", "cne": r.CNE, "status": r.Etat or "Pending"} for r in cursor.fetchall()]

    def save_bulk_presence(self, sid, p_list):
        cursor = self.conn.cursor()
        try:
            for item in p_list:
                cursor.execute("UPDATE Presence SET Etat=?, DateEnregistrement=GETDATE() WHERE SeanceID=? AND EtudiantID=?", (item['status'], sid, item['student_id']))
                if cursor.rowcount == 0: cursor.execute("INSERT INTO Presence (SeanceID, EtudiantID, Etat) VALUES (?,?,?)", (sid, item['student_id'], item['status']))
            self.conn.commit(); return True
        except Exception: return False