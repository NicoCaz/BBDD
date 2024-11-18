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

    
class Guardia(BaseModel):
    id_guardia: int
    dia: datetime
    turno: str
    nro_matricula_medico: Optional[str] = None  # Médico asignado
    especialidad: Optional[str] = None          # Especialidad requerida

# Función para conectar a la base de datos
async def get_db():
    conn = oracledb.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

@app.get("/guardias/", response_model=List[Guardia])
async def get_guardias(
    fecha_inicio: Optional[str] = None,  # Fecha de inicio en formato YYYY-MM-DD
    fecha_fin: Optional[str] = None,    # Fecha de fin en formato YYYY-MM-DD
    db: oracledb.Connection = Depends(get_db)
):
    try:
        cursor = db.cursor()
        params = []
        query = """
        SELECT 
            g.id_guardia, 
            g.dia, 
            g.turno, 
            ta.nro_matricula_medico,
            e.nombre AS especialidad
        FROM HOSPITAL.Guardia g
        LEFT JOIN HOSPITAL.Tiene_Asignada ta ON g.id_guardia = ta.id_guardia
        LEFT JOIN HOSPITAL.Esta_asociada ea ON ta.nro_matricula_medico = ea.nro_matricula_medico
        LEFT JOIN HOSPITAL.Especialidad e ON ea.id_especialidad = e.id_especialidad
        WHERE 1=1
        """

        # Filtro por rango de fechas, solo si se pasan los parámetros
        if fecha_inicio:
            query += " AND TRUNC(g.dia) >= TO_DATE(:1, 'YYYY-MM-DD')"
            params.append(fecha_inicio)
        if fecha_fin:
            query += " AND TRUNC(g.dia) <= TO_DATE(:2, 'YYYY-MM-DD')"
            params.append(fecha_fin)

        query += " ORDER BY g.dia, g.turno"
        
        # Ejecutar consulta
        cursor.execute(query, params)
        guardias = [
            Guardia(
                id_guardia=row[0],
                dia=row[1],
                turno=row[2],
                nro_matricula_medico=row[3],
                especialidad=row[4]
            )
            for row in cursor
        ]
        
        return guardias

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

        # Obtener la fecha de la guardia
        cursor.execute("""
            SELECT g.dia
            FROM HOSPITAL.Guardia g
            WHERE g.id_guardia = :1
        """, [asignacion.id_guardia])
        result = cursor.fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Fecha de la guardia no encontrada")

        fecha_guardia = result[0]

        # Verificar si el médico está de vacaciones en la fecha de la guardia
        cursor.execute("""
            SELECT COUNT(*)
            FROM HOSPITAL.Vacacion v
            WHERE v.nro_matricula_medico = :1
            AND :2 BETWEEN v.fecha_inicio AND v.fecha_fin
        """, [asignacion.nro_matricula_medico, fecha_guardia])

        if cursor.fetchone()[0] > 0:
            raise HTTPException(status_code=400, detail="El médico está de vacaciones en la fecha de la guardia")

        # Verificar si el médico existe
        cursor.execute(
            "SELECT COUNT(*) FROM HOSPITAL.Medico WHERE nro_matricula = :1",
            [asignacion.nro_matricula_medico]
        )
        if cursor.fetchone()[0] == 0:
            raise HTTPException(status_code=404, detail="Médico no encontrado")

        # Obtener el mes y el año de la guardia a asignar
        cursor.execute("""
            SELECT EXTRACT(MONTH FROM g.dia), EXTRACT(YEAR FROM g.dia), g.id_especialidad
            FROM HOSPITAL.Guardia g
            WHERE g.id_guardia = :1
        """, [asignacion.id_guardia])

        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Guardia no encontrada en la base de datos")
        
        mes_guardia, anio_guardia, id_especialidad_guardia = result

        # Verificar si el médico tiene la especialidad de la guardia
        cursor.execute("""
            SELECT COUNT(*)
            FROM HOSPITAL.Esta_asociada ea
            WHERE ea.nro_matricula_medico = :1
            AND ea.id_especialidad = :2
        """, [asignacion.nro_matricula_medico, id_especialidad_guardia])

        if cursor.fetchone()[0] == 0:
            raise HTTPException(status_code=400, detail="El médico no tiene la especialidad requerida para esta guardia")

        # Obtener la cantidad de guardias que ya tiene asignadas el médico en el mismo mes y año
        cursor.execute("""
            SELECT COUNT(*) 
            FROM HOSPITAL.Tiene_Asignada ta
            JOIN HOSPITAL.Guardia g ON ta.id_guardia = g.id_guardia
            WHERE ta.nro_matricula_medico = :1
            AND EXTRACT(MONTH FROM g.dia) = :2
            AND EXTRACT(YEAR FROM g.dia) = :3
        """, [asignacion.nro_matricula_medico, mes_guardia, anio_guardia])

        cantidad_guardias = cursor.fetchone()[0]

        # Obtener el límite de guardias por mes para el médico
        cursor.execute("""
            SELECT CANTIDADGUARDIASMES 
            FROM HOSPITAL.Medico
            WHERE nro_matricula = :1
        """, [asignacion.nro_matricula_medico])

        cantidad_guardias_mes = cursor.fetchone()[0]

        # Validar que el médico no exceda el límite de guardias en ese mes
        if cantidad_guardias >= cantidad_guardias_mes:
            raise HTTPException(
                status_code=400,
                detail=f"El médico ya tiene asignadas {cantidad_guardias} guardias en el mes {mes_guardia}/{anio_guardia}, superando el límite de {cantidad_guardias_mes}."
            )

        # Verificar si el médico ya está asignado a la guardia
        cursor.execute("""
            SELECT COUNT(*)
            FROM HOSPITAL.Tiene_Asignada ta
            WHERE ta.id_guardia = :1 AND ta.nro_matricula_medico = :2
        """, [asignacion.id_guardia, asignacion.nro_matricula_medico])

        if cursor.fetchone()[0] > 0:
            raise HTTPException(status_code=400, detail="El médico ya está asignado a esta guardia")

        # Iniciar transacción
        try:
            # Crear nueva asignación en la tabla Tiene_Asignada (sin hacer UPDATE)
            cursor.execute("""
                INSERT INTO HOSPITAL.Tiene_Asignada (id_guardia, nro_matricula_medico)
                VALUES (:1, :2)
            """, [asignacion.id_guardia, asignacion.nro_matricula_medico])

            # Registrar en la tabla Administra
            cursor.execute("""
                INSERT INTO HOSPITAL.Administra 
                (id_administrador, id_guardia, fecha, descripcion, tipo_operacion)
                VALUES (:1, :2, SYSTIMESTAMP, :3, :4)
            """, [
                asignacion.id_administrador,
                asignacion.id_guardia,
                asignacion.descripcion,
                'CREACION'
            ])

            # Commit de la transacción
            db.commit()

            return {"message": "Asignación de guardia exitosa", "tipo_operacion": 'CREACION'}

        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Error en la transacción: {str(e)}")

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
