from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from datetime import datetime
import oracledb
from typing import List, Optional
import os
from dotenv import load_dotenv 
# Cargar variables de entorno
load_dotenv()

app = FastAPI(title="Hospital API - Administración de Guardias")

# Configuración de la base de datos
DB_CONFIG = {
    "user": os.getenv("DB_USER", "HOSPITAL"),
    "password": os.getenv("DB_PASSWORD", "oracle"),
    "dsn": os.getenv("DB_DSN", "localhost:1521/XEPDB1")
}

# Modelos Pydantic
class Administrador(BaseModel):
    id_administrador: int
    nombre: str

class AdministradorCreate(BaseModel):
    nombre: str

class AsignacionGuardia(BaseModel):
    id_guardia: int
    nro_matricula_medico: str
    id_administrador: int
    tipo_operacion: str
    descripcion: Optional[str] = None

class Medico(BaseModel):
    nro_matricula: str
    dni: str
    nombre: str
    apellido: str
    fecha_ingreso: datetime
    cuit_cuil: str
    cantidadGuardiasMes: int
    especialidades: List[str] = []

class MedicoSimple(BaseModel):
    nro_matricula: str
    nombre_completo: str
    especialidades: List[str] = []


# Función para conectar a la base de datos
async def get_db():
    conn = oracledb.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

# Endpoints para Administradores
@app.get("/administradores/", response_model=List[Administrador])
async def get_administradores(db: oracledb.Connection = Depends(get_db)):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT id_administrador, nombre FROM HOSPITAL.Administrador ORDER BY id_administrador")
        administradores = cursor.fetchall()
        return [
            Administrador(id_administrador=admin[0], nombre=admin[1])
            for admin in administradores
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/administradores/", response_model=Administrador)
async def create_administrador(admin: AdministradorCreate, db: oracledb.Connection = Depends(get_db)):
    try:
        cursor = db.cursor()
        
        # Obtener el siguiente ID disponible
        cursor.execute("SELECT NVL(MAX(id_administrador), 0) + 1 FROM HOSPITAL.Administrador")
        next_id = cursor.fetchone()[0]
        
        # Insertar nuevo administrador
        cursor.execute(
            "INSERT INTO HOSPITAL.Administrador (id_administrador, nombre) VALUES (:1, :2)",
            [next_id, admin.nombre]
        )
        db.commit()
        
        return Administrador(id_administrador=next_id, nombre=admin.nombre)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/medicos/", response_model=List[MedicoSimple])
async def get_medicos(db: oracledb.Connection = Depends(get_db)):
    try:
        cursor = db.cursor()
        
        # Consulta corregida
        query = """
        WITH MedicoEspecialidades AS (
            SELECT 
                m.nro_matricula,
                p.nombre || ' ' || p.apellido as nombre_completo,
                LISTAGG(e.nombre, ', ') 
                    WITHIN GROUP (ORDER BY e.nombre) OVER (PARTITION BY m.nro_matricula) as especialidades
            FROM HOSPITAL.Medico m
            JOIN HOSPITAL.Persona p ON m.dni = p.dni
            LEFT JOIN HOSPITAL.Esta_asociada ea ON m.nro_matricula = ea.nro_matricula_medico
            LEFT JOIN HOSPITAL.Especialidad e ON ea.id_especialidad = e.id_especialidad
        )
        SELECT DISTINCT 
            nro_matricula,
            nombre_completo,
            especialidades
        FROM MedicoEspecialidades
        ORDER BY nombre_completo
        """
        
        cursor.execute(query)
        medicos = []
        
        for row in cursor:
            especialidades = row[2].split(', ') if row[2] else []
            medicos.append({
                "nro_matricula": row[0],
                "nombre_completo": row[1],
                "especialidades": especialidades
            })
            
        return medicos
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/medicos/{nro_matricula}", response_model=Medico)
async def get_medico_by_matricula(nro_matricula: str, db: oracledb.Connection = Depends(get_db)):
    try:
        cursor = db.cursor()
        
        # Consulta corregida
        query = """
        WITH MedicoEspecialidades AS (
            SELECT 
                m.nro_matricula,
                m.dni,
                p.nombre,
                p.apellido,
                m.fecha_ingreso,
                m.cuit_cuil,
                m.cantidadGuardiasMes,
                LISTAGG(e.nombre, ', ') 
                    WITHIN GROUP (ORDER BY e.nombre) OVER (PARTITION BY m.nro_matricula) as especialidades
            FROM HOSPITAL.Medico m
            JOIN HOSPITAL.Persona p ON m.dni = p.dni
            LEFT JOIN HOSPITAL.Esta_asociada ea ON m.nro_matricula = ea.nro_matricula_medico
            LEFT JOIN HOSPITAL.Especialidad e ON ea.id_especialidad = e.id_especialidad
            WHERE m.nro_matricula = :1
        )
        SELECT DISTINCT 
            nro_matricula,
            dni,
            nombre,
            apellido,
            fecha_ingreso,
            cuit_cuil,
            cantidadGuardiasMes,
            especialidades
        FROM MedicoEspecialidades
        """
        
        cursor.execute(query, [nro_matricula])
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Médico no encontrado")
            
        especialidades = row[7].split(', ') if row[7] else []
        
        return {
            "nro_matricula": row[0],
            "dni": row[1],
            "nombre": row[2],
            "apellido": row[3],
            "fecha_ingreso": row[4],
            "cuit_cuil": row[5],
            "cantidadGuardiasMes": row[6],
            "especialidades": especialidades
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/medicos/disponibles/{fecha}", response_model=List[MedicoSimple])
async def get_medicos_disponibles(
    fecha: str,  # Formato: YYYY-MM-DD
    especialidad_id: Optional[int] = None,
    db: oracledb.Connection = Depends(get_db)
):
    try:
        cursor = db.cursor()
        
        # Consulta corregida
        query = """
        WITH MedicoEspecialidades AS (
            SELECT DISTINCT
                m.nro_matricula,
                p.nombre || ' ' || p.apellido as nombre_completo,
                LISTAGG(e.nombre, ', ') 
                    WITHIN GROUP (ORDER BY e.nombre) OVER (PARTITION BY m.nro_matricula) as especialidades
            FROM HOSPITAL.Medico m
            JOIN HOSPITAL.Persona p ON m.dni = p.dni
            JOIN HOSPITAL.Esta_asociada ea ON m.nro_matricula = ea.nro_matricula_medico
            JOIN HOSPITAL.Especialidad e ON ea.id_especialidad = e.id_especialidad
            WHERE ea.disponible_guardia = 1
            AND NOT EXISTS (
                SELECT 1 FROM HOSPITAL.Vacacion v
                WHERE v.nro_matricula = m.nro_matricula
                AND TO_DATE(:1, 'YYYY-MM-DD') BETWEEN v.fecha_inicio AND v.fecha_fin
            )
            AND NOT EXISTS (
                SELECT 1 FROM HOSPITAL.Tiene_Asignada ta
                JOIN HOSPITAL.Guardia g ON ta.id_guardia = g.id_guardia
                WHERE ta.nro_matricula_medico = m.nro_matricula
                AND TRUNC(g.dia) = TO_DATE(:1, 'YYYY-MM-DD')
            )
        """
        
        params = [fecha]
        
        if especialidad_id is not None:
            query += " AND ea.id_especialidad = :2"
            params.append(especialidad_id)
            
        query += """
        )
        SELECT DISTINCT 
            nro_matricula,
            nombre_completo,
            especialidades
        FROM MedicoEspecialidades
        ORDER BY nombre_completo
        """
        
        cursor.execute(query, params)
        medicos = []
        
        for row in cursor:
            especialidades = row[2].split(', ') if row[2] else []
            medicos.append({
                "nro_matricula": row[0],
                "nombre_completo": row[1],
                "especialidades": especialidades
            })
            
        return medicos
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# Endpoint para asignar/modificar guardia
@app.post("/guardias/asignar/")
async def asignar_guardia(asignacion: AsignacionGuardia, db: oracledb.Connection = Depends(get_db)):
    try:
        cursor = db.cursor()
        
        # Verificar si la guardia existe
        cursor.execute(
            "SELECT COUNT(*) FROM HOSPITAL.Guardia WHERE id_guardia = :1",
            [asignacion.id_guardia]
        )
        if cursor.fetchone()[0] == 0:
            raise HTTPException(status_code=404, detail="Guardia no encontrada")

        # Verificar si el médico existe
        cursor.execute(
            "SELECT COUNT(*) FROM HOSPITAL.Medico WHERE nro_matricula = :1",
            [asignacion.nro_matricula_medico]
        )
        if cursor.fetchone()[0] == 0:
            raise HTTPException(status_code=404, detail="Médico no encontrado")

        # Verificar si ya existe una asignación
        cursor.execute("""
            SELECT COUNT(*) 
            FROM HOSPITAL.Tiene_Asignada 
            WHERE id_guardia = :1
        """, [asignacion.id_guardia])
        
        asignacion_existe = cursor.fetchone()[0] > 0
        
        # Iniciar transacción
        try:
            if not asignacion_existe:
                # Crear nueva asignación
                cursor.execute("""
                    INSERT INTO HOSPITAL.Tiene_Asignada (id_guardia, nro_matricula_medico)
                    VALUES (:1, :2)
                """, [asignacion.id_guardia, asignacion.nro_matricula_medico])
            else:
                # Actualizar asignación existente
                cursor.execute("""
                    UPDATE HOSPITAL.Tiene_Asignada 
                    SET nro_matricula_medico = :1
                    WHERE id_guardia = :2
                """, [asignacion.nro_matricula_medico, asignacion.id_guardia])

            # Registrar en la tabla Administra
            cursor.execute("""
                INSERT INTO HOSPITAL.Administra 
                (id_administrador, id_guardia, fecha, descripcion, tipo_operacion)
                VALUES (:1, :2, SYSTIMESTAMP, :3, :4)
            """, [
                asignacion.id_administrador,
                asignacion.id_guardia,
                asignacion.descripcion,
                'CREACION' if not asignacion_existe else 'MODIFICACION'
            ])

            # Commit de la transacción
            db.commit()

            return {
                "message": "Asignación de guardia exitosa",
                "tipo_operacion": 'CREACION' if not asignacion_existe else 'MODIFICACION'
            }

        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Error en la transacción: {str(e)}")

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Validaciones adicionales
@app.get("/guardias/validar-asignacion/{id_guardia}/{nro_matricula}/")
async def validar_asignacion_guardia(
    id_guardia: int,
    nro_matricula: str,
    db: oracledb.Connection = Depends(get_db)
):
    try:
        cursor = db.cursor()
        
        # 1. Verificar si el médico está de vacaciones en la fecha de la guardia
        cursor.execute("""
            SELECT COUNT(*)
            FROM HOSPITAL.Guardia g
            JOIN HOSPITAL.Vacacion v ON g.dia BETWEEN v.fecha_inicio AND v.fecha_fin
            WHERE g.id_guardia = :1 AND v.nro_matricula = :2
        """, [id_guardia, nro_matricula])
        
        if cursor.fetchone()[0] > 0:
            return {"valid": False, "reason": "El médico está de vacaciones en la fecha de la guardia"}

        # 2. Verificar cantidad de guardias del mes
        cursor.execute("""
            SELECT 
                m.cantidadGuardiasMes,
                COUNT(ta.id_guardia) as guardias_asignadas
            FROM HOSPITAL.Medico m
            LEFT JOIN HOSPITAL.Tiene_Asignada ta ON m.nro_matricula = ta.nro_matricula_medico
            LEFT JOIN HOSPITAL.Guardia g ON ta.id_guardia = g.id_guardia
                AND EXTRACT(MONTH FROM g.dia) = EXTRACT(MONTH FROM (
                    SELECT dia FROM HOSPITAL.Guardia WHERE id_guardia = :1
                ))
                AND EXTRACT(YEAR FROM g.dia) = EXTRACT(YEAR FROM (
                    SELECT dia FROM HOSPITAL.Guardia WHERE id_guardia = :1
                ))
            WHERE m.nro_matricula = :2
            GROUP BY m.cantidadGuardiasMes
        """, [id_guardia, nro_matricula])
        
        row = cursor.fetchone()
        if row and row[1] >= row[0]:
            return {"valid": False, "reason": "El médico ya alcanzó su máximo de guardias mensuales"}

        return {"valid": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))