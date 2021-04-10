from multiprocessing import Process
import cx_Oracle
import datetime
import shutil
import json
import time
import gzip
# import bz2

try:
    from Parametre import *
except ModuleNotFoundError:
    print("Module de parametrage non trouvé")
    exit(1)
try:
    with open(spectre_path, 'r') as fjson: spectre = json.load(fjson)
except Exception as e:
    print("Ficher de spectre non trouvé")
    print(e)
    exit(2)

dsn = cx_Oracle.makedsn(db_ip, db_port, sid=db_sid)
if not os.path.isdir(rep_input): os.makedirs(rep_input)
if not os.path.isdir(rep_backup): os.makedirs(rep_backup)
if not os.path.isdir(rep_rejets): os.makedirs(rep_rejets)
if not os.path.isdir(rep_logs): os.makedirs(rep_logs)


class LoadingCDR(Process):

    def __init__(self, fileslist, numprocess):
        Process.__init__(self)
        self.fileslist = fileslist
        self.numprocess = numprocess

    def chunk_list(self, liste, nbssListe):
        GL = []
        try:
            nb = len(liste) // nbssListe
            i = 0
            while i < nbssListe:
                GL.append([])
                for elem in liste[nb * i:nb * (i + 1)]:
                    GL[i].append(elem)
                i += 1
            if nb * i < len(liste):
                depart = nb * i
                [GL[i - depart].append(liste[i]) for i in range(nb * i, len(liste))]
        except ZeroDivisionError:
            print('le nombre de process doit être supérieur à 0')
            [GL.append(elem) for elem in liste]
        return GL

    def read_file(self, file, ext):

        if ext in ['DAT', 'dat']:
            with open(file, 'r') as f:
                file_content = f.readlines()
#        elif ext == 'bz2':
#            with bz2.open(file, 'rt') as f:
#                file_content = f.readlines()
        elif ext == 'gz':
            with gzip.open(file, 'rt') as f:
                file_content = f.readlines()
        else:
            file_content = []

        return file_content

    def split_cdr_to_list(self, cdr, spectre, filename):
        splited_cdr = []
        for champ, position in spectre.items():
            splited_cdr.append(cdr[position[0]:position[1]].strip())

        splited_cdr.append(filename)
        splited_cdr.append(datetime.datetime.now())

        return splited_cdr

    def split_cdr_to_dict(self, cdr, spectre, filename):
        splited_cdr = dict()
        for champ, position in spectre.items():
            splited_cdr[champ] = cdr[position[0]:position[1]].strip()

        splited_cdr["filename"] = filename
        splited_cdr["date_load"] = datetime.datetime.now()

        return splited_cdr

    def get_table_name(self, prefix, date):
        try:
            dt = date[6:8] + date[4:6] + date[2:4]
        except IndexError:
            dt = ""

        return prefix + dt

    def table_exists(self, table_name):
        req_veriftable = """SELECT count(distinct TABLE_NAME) FROM dba_tables 
                            WHERE owner = 'DWH' and upper(TABLE_NAME) = '{}'""".format(table_name.upper())

        # print(req_veriftable)
        with cx_Oracle.connect(db_user, db_pwd, dsn, encoding='UTF-8') as cnx:
            cur = cnx.cursor()
            cur.execute(req_veriftable)
            for row in cur:
                nb_table = row[0]
            cur.close()

        return True if nb_table == 1 else False

    def create_table(self, table_name, spectre):
        req_create_table = "CREATE TABLE " + table_name + " ( "
        for champ, position in spectre.items():
            req_create_table += champ + "  VARCHAR2(" + str(position[1] - position[0]) + "),"
        req_create_table += "filename VARCHAR2(30), date_load DATE )"

        # print(req)
        with cx_Oracle.connect(db_user, db_pwd, dsn, encoding='UTF-8') as cnx:
            cur = cnx.cursor()
            cur.execute(req_create_table)
            cur.close()

    def load_data_in_table(self, table_name, cdrs):
        # VERIFIER L'EXISTENCE DE LA TABLE
        if not self.table_exists(table_name):
            self.create_table(table_name, spectre)

        # FORMATION DE LA LISTE DES CHAMPS ET LES VARIABLES DE LIAISONS
        table_fields = ""
        bind_fields = ""
        for i in spectre.keys():
            table_fields += i + ", "
            bind_fields += ":" + i + ", "
        table_fields += " filename, date_load"
        bind_fields += " :filename, :date_load"

        # REQUETE
        insert_query = """ insert into {} ({}) values ({}) """.format(table_name, table_fields, bind_fields)
        cnx_ld = "cnx_ld" + str(self.numprocess)
        # print(insert_query)
        with cx_Oracle.connect(db_user, db_pwd, dsn, encoding='UTF-8') as cnx:
            cur = cnx.cursor()
            cur.executemany(insert_query, cdrs)
            cnx.commit()
            cur.close()

        cdrs.clear()

    def move_file(self, block_files, dest):
        for file in block_files:
            name_fic = os.path.basename(file)
            try:
                shutil.move(file, dest)
            except FileExistsError:
                print("le fichier {} exite deja dans le backup".format(name_fic))
                shutil.move(file, dest + name_fic + "_bis")
            except PermissionError:
                shutil.move(file, rep_input + name_fic + "_traite")

        block_files.clear()

    def compress_file(self, block_files, dest):
        for file in block_files:
            name_fic = os.path.basename(file)
            with open(rep_backup + name_fic, 'rb') as f_in:
                with gzip.open(dest + name_fic + '.gz', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        block_files.clear()

    def write_to_file(self, a_ecrire, namefile):
        with open(namefile, 'a') as fic:
            fic.write('\n'.join(a_ecrire) + '\n')
        a_ecrire.clear()

    def run(self):
        print("Debut du process {:3} avec {:8} fichier(s)".format(self.numprocess, len(self.fileslist)))
        debut_tmt = time.time()
        dict_of_cdrs = {}
        bad_cdrs = []
        loaded_file = []
        logs = []
        nb_file = len(self.fileslist)
        nb_current_file = 0
        nb_attempt_file = 0

        for fichier in self.fileslist:
            filename = os.path.basename(fichier)
            # print(filename)
            ext = filename.split('.')[-1]
            date_debut = time.strftime('%Y-%m-%d %H:%M:%S')
            start = time.time()

            file_content = self.read_file(fichier, ext)

            if len(file_content) > 0:
                for cdr in file_content:
                    try:
                        splited_cdr = self.split_cdr_to_dict(cdr, spectre, filename)
                        # table_name = prefix_table_name + "_" + splited_cdr['calldate']
                        table_name = self.get_table_name(prefix_table_name, splited_cdr['calldate'])
                    except IndexError:
                        bad_cdrs.append(filename + " | " + cdr)
                        splited_cdr = {}
                        table_name = prefix_table_name + "_xxxxxx"

                    if not (table_name in dict_of_cdrs.keys()):
                        dict_of_cdrs[table_name] = []
                    dict_of_cdrs[table_name].append(splited_cdr)

                loaded_file.append(fichier)

            date_fin = time.strftime('%Y-%m-%d %H:%M:%S')
            duree = str(time.time() - start).split('.')[0]

            nb_current_file += 1
            nb_attempt_file += 1

            logs.append(";".join([filename, date_debut, date_fin, duree, str(len(file_content))]))

            if nb_attempt_file >= nb_file_to_load or nb_current_file == nb_file:
                print("chargement du process {} au point {}".format(self.numprocess, nb_current_file))
                [self.load_data_in_table(table, cdrs) for table, cdrs in dict_of_cdrs.items()]
                if len(bad_cdrs) > 0: self.write_to_file(bad_cdrs, rep_rejets + "/BADCDR_" + time.strftime('%Y%m%d'))
                self.write_to_file(logs, rep_logs + prefix_file_log + str(self.numprocess) + "_" + time.strftime('%Y-%m-%d'))
                self.move_file(loaded_file, rep_backup)
                nb_attempt_file = 0

        duree = str(time.time() - debut_tmt).split(".")[0]
        print("le traitement des {:8} fichier(s) du process {:3} a pris {:9} seconde(s)".format(nb_file, self.numprocess, duree))
        # Compression des fichiers traités
        # self.compress_file(self.fileslist, rep_backup)
