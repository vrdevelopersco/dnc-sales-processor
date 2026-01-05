#!/bin/bash
# Configuración
USER="anakin0"
DB_NAME="dnc_processor"
BACKUP_DIR="/media/bodega/backups"
DATE=$(date +%Y-%m-%d_%H%M)

# Crear carpeta si no existe
mkdir -p $BACKUP_DIR

# Ejecutar el dump
echo "Iniciando backup de $DB_NAME..."
pg_dump -U $USER -d $DB_NAME | gzip > $BACKUP_DIR/db_backup_$DATE.sql.gz

# Limpieza: Borrar backups más viejos de 7 días para no llenar el disco
find $BACKUP_DIR -type f -name "*.sql.gz" -mtime +7 -delete

echo "Backup completado exitosamente en $BACKUP_DIR/db_backup_$DATE.sql.gz"

