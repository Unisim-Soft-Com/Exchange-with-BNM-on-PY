import sys
import requests
import cx_Oracle
from datetime import datetime
import xml.etree.ElementTree as ET

sys.stdout.reconfigure(encoding='utf-8')

def load_xml_exchange_data(date, lang):
    """
    Extrage datele XML de la BNM pentru o anumită dată și limbă.
    URL-ul construit este de forma:
    http://www.bnm.md/{lang}/official_exchange_rates?get_xml=1&date={data}
    """
    formatted_date = date.strftime('%d.%m.%Y')
    url = f'http://www.bnm.md/{lang}/official_exchange_rates?get_xml=1&date={formatted_date}'
    response = requests.get(url)
    if response.status_code == 200:
        print("Răspunsul de la server primit cu succes.")
        return response.text
    else:
        print(f"Eroare la descărcarea datelor. Status code: {response.status_code}")
        return None

def parse_xml_data(xml_data):
    """
    Parsează XML-ul primit și extrage ratele de schimb pentru valutele dorite.
    Se așteaptă ca XML-ul să conțină elemente <Valute> cu subelemente <CharCode> și <Value>.
    """
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"Eroare la parsarea XML-ului: {e}")
        return None

    data_list = []
    desired_currencies = {'USD', 'EUR', 'GBP', 'RUB', 'UAH'}
    current_date = datetime.now().strftime('%Y.%m.%d')

    for valute in root.findall('Valute'):
        char_code_elem = valute.find('CharCode')
        value_elem = valute.find('Value')
        if char_code_elem is not None and value_elem is not None:
            char_code = char_code_elem.text.strip()
            value_text = value_elem.text.strip()
            if char_code in desired_currencies:
                butype = ' '
                nrset = 0
                try:
                    curs = float(value_text.replace(',', '.'))
                except ValueError:
                    print(f"Nu s-a putut converti valoarea '{value_text}' la float pentru moneda {char_code}.")
                    continue
                data_list.append((butype, nrset, current_date, char_code, curs))
    return data_list

def display_data_in_console(data):
    print("Datele obținute sunt:")
    for record in data:
        print(f"BUTYPE: {record[0]}, NRSET: {record[1]}, DATA: {record[2]}, VALUTA: {record[3]}, CURS: {record[4]}")

def update_or_insert_data_into_oracle(data):
    connection = None
    cursor = None
    try:
        connection = cx_Oracle.connect(user="fco2023", password="fco2023",
                                       dsn="93.115.136.18:4024/clouddev.world")
        cursor = connection.cursor()

        for record in data:
            select_query = """
            SELECT COUNT(*) 
            FROM TXCURS 
            WHERE DATA = TO_DATE(:data, 'YYYY.MM.DD') AND NRSET = :nrset AND VALUTA = :valuta
            """
            cursor.execute(select_query, data=record[2], nrset=record[1], valuta=record[3])
            count = cursor.fetchone()[0]

            if count > 0:
                update_query = """
                UPDATE TXCURS 
                SET CURS = :curs 
                WHERE DATA = TO_DATE(:data, 'YYYY.MM.DD') AND NRSET = :nrset AND VALUTA = :valuta
                """
                cursor.execute(update_query, curs=record[4], data=record[2], nrset=record[1], valuta=record[3])
                print(f"Am actualizat CURS-ul pentru {record[3]} la DATA {record[2]} și NRSET {record[1]}.")
            else:
                insert_query = """
                INSERT INTO TXCURS (BUTYPE, NRSET, DATA, VALUTA, CURS)
                VALUES (:butype, :nrset, TO_DATE(:data, 'YYYY.MM.DD'), :valuta, :curs)
                """
                cursor.execute(insert_query, butype=record[0], nrset=record[1],
                               data=record[2], valuta=record[3], curs=record[4])
                print(f"Am inserat noi date pentru {record[3]} la DATA {record[2]} și NRSET {record[1]}.")

        mdl_record = (' ', 0, datetime.now().strftime('%Y.%m.%d'), 'MDL', 1.0)
        insert_query = """
        INSERT INTO TXCURS (BUTYPE, NRSET, DATA, VALUTA, CURS)
        VALUES (:butype, :nrset, TO_DATE(:data, 'YYYY.MM.DD'), :valuta, :curs)
        """
        cursor.execute(insert_query, butype=mdl_record[0], nrset=mdl_record[1],
                       data=mdl_record[2], valuta=mdl_record[3], curs=mdl_record[4])
        print(f"Am inserat MDL cu valoarea 1 la DATA {mdl_record[2]} și NRSET {mdl_record[1]}.")

        connection.commit()
        print("Modificările au fost confirmate cu succes (commit efectuat).")

    except cx_Oracle.DatabaseError as e:
        print("Eroare la actualizarea sau inserarea în baza de date:", e)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def main():
    today = datetime.now()
    lang = 'en'
    
    xml_data = load_xml_exchange_data(today, lang)
    
    if xml_data:
        data = parse_xml_data(xml_data)
        if data:
            display_data_in_console(data)
            update_or_insert_data_into_oracle(data)
        else:
            print("Nu s-au extras date din XML.")
    else:
        print("Nu s-au obținut date de la URL.")

if __name__ == "__main__":
    main()
