#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de scraping para SECO Sanctions (Swiss SESAM) - Version Codespaces.
Usa Playwright (funciona en contenedores). No requiere captcha.

Uso:
    python scrape_seco_codespace.py "NOMBRE COMPLETO" ["Person"|"Organisation"]

Variables de entorno opcionales:
    (ninguna requerida)
"""

import sys
import json
from playwright.sync_api import sync_playwright


URL_SECO = "https://www.sesam.search.admin.ch/sesam-search-web/pages/search.xhtml?lang=en"


def buscar_sanciones(nombre_completo: str, subject_type: str = "Person") -> dict:
    """Realiza la busqueda de sanciones SECO por nombre."""

    with sync_playwright() as p:
        browser = None
        try:
            print("Iniciando navegador...", file=sys.stderr)
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )

            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                )
            )
            page = context.new_page()
            page.set_default_timeout(60000)

            # 1. Navegar a la pagina
            print(f"Navegando a: {URL_SECO}", file=sys.stderr)
            page.goto(URL_SECO, wait_until='domcontentloaded')
            page.wait_for_timeout(5000)

            # 2. Encontrar y llenar el campo de nombre
            print(f"Ingresando nombre: {nombre_completo}", file=sys.stderr)
            name_input = _find_name_input(page)
            if not name_input:
                return {
                    'status': 'error',
                    'message': 'No se pudo encontrar el campo de nombre en la pagina'
                }
            name_input.fill(nombre_completo)
            page.wait_for_timeout(1000)

            # 3. Seleccionar tipo de sujeto
            print(f"Seleccionando tipo de sujeto: {subject_type}", file=sys.stderr)
            _select_subject_type(page, subject_type)
            page.wait_for_timeout(1000)

            # 4. Click en buscar
            print("Haciendo click en buscar...", file=sys.stderr)
            search_clicked = _click_search_button(page)
            if not search_clicked:
                return {
                    'status': 'error',
                    'message': 'No se pudo encontrar el boton de busqueda'
                }

            # 5. Esperar resultados
            print("Esperando resultados...", file=sys.stderr)
            page.wait_for_timeout(8000)
            page.wait_for_load_state('networkidle')
            page.wait_for_timeout(2000)

            # 6. Extraer resultados
            print("Extrayendo resultados...", file=sys.stderr)
            body_text = page.inner_text("body")

            # Verificar si no hay resultados
            no_results_phrases = [
                "no matches found",
                "no results",
                "keine treffer",
                "aucun resultat",
                "0 matches",
                "no entries found",
            ]
            body_lower = body_text.lower()
            has_no_results = any(phrase in body_lower for phrase in no_results_phrases)

            if has_no_results:
                print("No se encontraron resultados", file=sys.stderr)
                return {
                    'status': 'success',
                    'message': 'Busqueda de sanciones SECO realizada correctamente',
                    'datos': {
                        'tiene_resultados': False,
                        'total_resultados': 0,
                        'mensaje': 'No se encontraron resultados',
                        'resultados': []
                    },
                    'parametros_busqueda': {
                        'name': nombre_completo,
                        'subject_type': subject_type
                    }
                }

            # Buscar tabla de resultados
            resultados = _extract_table_results(page)

            total = len(resultados)
            print(f"Total resultados: {total}", file=sys.stderr)

            return {
                'status': 'success',
                'message': 'Busqueda de sanciones SECO realizada correctamente',
                'datos': {
                    'tiene_resultados': total > 0,
                    'total_resultados': total,
                    'mensaje': f'Se encontraron {total} resultado(s)',
                    'resultados': resultados
                },
                'parametros_busqueda': {
                    'name': nombre_completo,
                    'subject_type': subject_type
                }
            }

        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)[:300]
            }
        finally:
            if browser:
                browser.close()


def _find_name_input(page):
    """Encuentra el campo de entrada de nombre usando varios selectores."""
    selectors = [
        "input[id*='name']",
        "input[name*='name']",
        "input[placeholder*='name' i]",
        "input[id*='Name']",
        "input[name*='Name']",
        "input[placeholder*='Name']",
    ]

    for selector in selectors:
        try:
            el = page.locator(selector).first
            if el.count() > 0 and el.is_visible():
                print(f"  Campo de nombre encontrado con selector: {selector}", file=sys.stderr)
                return el
        except Exception:
            continue

    # Fallback: primer input de texto visible
    try:
        inputs = page.locator("input[type='text']").all()
        for inp in inputs:
            if inp.is_visible():
                print("  Campo de nombre encontrado por fallback (primer input text)", file=sys.stderr)
                return inp
    except Exception:
        pass

    # Segundo fallback: cualquier input sin tipo especifico
    try:
        inputs = page.locator(
            "input:not([type='hidden']):not([type='submit'])"
            ":not([type='button']):not([type='checkbox'])"
            ":not([type='radio'])"
        ).all()
        for inp in inputs:
            if inp.is_visible():
                print("  Campo de nombre encontrado por fallback generico", file=sys.stderr)
                return inp
    except Exception:
        pass

    return None


def _select_subject_type(page, subject_type: str):
    """Selecciona el tipo de sujeto (Person/Organisation) en el dropdown."""
    # Intentar con selectores especificos de JSF/PrimeFaces
    select_selectors = [
        "select[id*='type']",
        "select[id*='subject']",
        "select[id*='Type']",
        "select[id*='Subject']",
        "select[id*='category']",
        "select[id*='Category']",
    ]

    for selector in select_selectors:
        try:
            el = page.locator(selector).first
            if el.count() > 0 and el.is_visible():
                print(f"  Dropdown encontrado con selector: {selector}", file=sys.stderr)
                # Intentar seleccionar por label
                try:
                    page.select_option(selector, label=subject_type)
                    print(f"  Tipo seleccionado por label: {subject_type}", file=sys.stderr)
                    return
                except Exception:
                    pass
                # Intentar seleccionar por valor
                try:
                    page.select_option(selector, value=subject_type)
                    print(f"  Tipo seleccionado por value: {subject_type}", file=sys.stderr)
                    return
                except Exception:
                    pass
        except Exception:
            continue

    # Fallback: buscar cualquier select visible
    try:
        selects = page.locator("select").all()
        for select in selects:
            if select.is_visible():
                options_text = select.inner_text()
                if 'person' in options_text.lower() or 'organisation' in options_text.lower():
                    try:
                        select.select_option(label=subject_type)
                        print(f"  Tipo seleccionado en select generico: {subject_type}", file=sys.stderr)
                        return
                    except Exception:
                        pass
    except Exception:
        pass

    # PrimeFaces dropdown fallback: div-based dropdown
    try:
        pf_selects = page.locator("div.ui-selectonemenu").all()
        for pf_sel in pf_selects:
            if pf_sel.is_visible():
                pf_sel.click()
                page.wait_for_timeout(500)
                option = page.locator(f"li:has-text('{subject_type}')").first
                if option.count() > 0:
                    option.click()
                    print(f"  Tipo seleccionado via PrimeFaces dropdown: {subject_type}", file=sys.stderr)
                    return
    except Exception:
        pass

    print(f"  Advertencia: No se pudo seleccionar tipo de sujeto: {subject_type}", file=sys.stderr)


def _click_search_button(page) -> bool:
    """Encuentra y hace click en el boton de busqueda."""
    selectors = [
        "button[id*='search']",
        "button[id*='Search']",
        "input[type='submit']",
        "button[type='submit']",
        "input[id*='search']",
        "input[id*='Search']",
    ]

    for selector in selectors:
        try:
            el = page.locator(selector).first
            if el.count() > 0 and el.is_visible():
                el.click()
                print(f"  Boton de busqueda clickeado con selector: {selector}", file=sys.stderr)
                return True
        except Exception:
            continue

    # Fallback: buscar por texto del boton
    text_patterns = [
        "Start search",
        "Search",
        "Suche starten",
        "Rechercher",
        "Submit",
    ]
    for text in text_patterns:
        try:
            btn = page.locator(f"button:has-text('{text}')").first
            if btn.count() > 0 and btn.is_visible():
                btn.click()
                print(f"  Boton de busqueda clickeado por texto: {text}", file=sys.stderr)
                return True
        except Exception:
            continue

    # Segundo fallback: input con value de busqueda
    for text in text_patterns:
        try:
            btn = page.locator(f"input[value='{text}']").first
            if btn.count() > 0 and btn.is_visible():
                btn.click()
                print(f"  Boton de busqueda clickeado por value: {text}", file=sys.stderr)
                return True
        except Exception:
            continue

    # Ultimo fallback: cualquier commandButton de PrimeFaces
    try:
        pf_btn = page.locator("button.ui-button").first
        if pf_btn.count() > 0 and pf_btn.is_visible():
            pf_btn.click()
            print("  Boton de busqueda clickeado via PrimeFaces button", file=sys.stderr)
            return True
    except Exception:
        pass

    return False


def _extract_table_results(page) -> list:
    """Extrae los resultados de la tabla de sanciones."""
    resultados = []

    # Intentar diferentes selectores de tabla
    table_selectors = [
        "table[role='grid']",
        "table.ui-datatable-tablewrapper table",
        "div.ui-datatable table",
        "table.ui-datatable",
        "table",
    ]

    rows = []
    for selector in table_selectors:
        try:
            found_rows = page.locator(f"{selector} tbody tr").all()
            if found_rows:
                print(f"  Tabla encontrada con selector: {selector} ({len(found_rows)} filas)", file=sys.stderr)
                rows = found_rows
                break
        except Exception:
            continue

    if not rows:
        # Intentar con role='row'
        try:
            rows = page.locator("[role='row']").all()
            # Filtrar header rows
            if rows:
                rows = rows[1:]  # Skip header
                print(f"  Filas encontradas por role=row: {len(rows)}", file=sys.stderr)
        except Exception:
            pass

    if not rows:
        print("  No se encontraron filas en la tabla", file=sys.stderr)
        # Verificar si hay contenido que indique resultados sin tabla
        body_text = page.inner_text("body").lower()
        if "match" in body_text or "result" in body_text:
            print("  Hay texto de resultados pero no se pudo extraer la tabla", file=sys.stderr)
        return []

    for idx, row in enumerate(rows, 1):
        try:
            cells = row.locator("td").all()
            if not cells or len(cells) < 2:
                continue

            resultado = {'numero': idx}

            # Extraer columnas - la estructura tipica de SESAM tiene:
            # nombre, tipo_sujeto, programa_sancion, calidad_coincidencia
            cell_texts = []
            for cell in cells:
                text = cell.inner_text().strip()
                cell_texts.append(text)

            if len(cell_texts) >= 1:
                resultado['nombre'] = cell_texts[0]
            if len(cell_texts) >= 2:
                resultado['tipo_sujeto'] = cell_texts[1]
            if len(cell_texts) >= 3:
                resultado['programa_sancion'] = cell_texts[2]
            if len(cell_texts) >= 4:
                resultado['calidad_coincidencia'] = cell_texts[3]

            # Si solo hay 2 columnas, intentar asignar de forma inteligente
            if len(cell_texts) == 2:
                resultado['nombre'] = cell_texts[0]
                resultado['detalle'] = cell_texts[1]

            # Solo agregar si tiene al menos nombre
            if resultado.get('nombre'):
                resultados.append(resultado)
                print(f"  Fila {idx}: {resultado.get('nombre', 'N/A')[:50]}", file=sys.stderr)

        except Exception as e:
            print(f"  Error en fila {idx}: {e}", file=sys.stderr)
            continue

    return resultados


def main():
    if len(sys.argv) < 2:
        print(json.dumps({
            'status': 'error',
            'message': 'Uso: python scrape_seco_codespace.py <nombre_completo> [Person|Organisation]'
        }))
        return

    nombre_completo = sys.argv[1]
    subject_type = sys.argv[2] if len(sys.argv) > 2 else "Person"

    # Validar tipo de sujeto
    valid_types = ["Person", "Organisation"]
    if subject_type not in valid_types:
        print(
            f"Advertencia: tipo '{subject_type}' no reconocido, usando 'Person'",
            file=sys.stderr
        )
        subject_type = "Person"

    result = buscar_sanciones(nombre_completo, subject_type)

    # Solo el JSON al stdout (los logs van a stderr)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
