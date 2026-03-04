#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de scraping para Movilidad Bogota (comparendos de transito) - Version Codespaces.
Usa Playwright + CapMonster (reCAPTCHA v2) + resolucion local de captcha matematico.

Uso:
    python scrape_movilidad_codespace.py "CEDULA" "1234567890"

Tipos de identificacion validos:
    CEDULA, CEDULA_EXTRANJERIA, NIT, PASAPORTE

Variables de entorno requeridas:
    CAPMONSTER_API_KEY: API key de CapMonster Cloud
"""

import sys
import os
import json
import re
import time
import operator
import requests
from playwright.sync_api import sync_playwright


# Configuracion
CAPMONSTER_API_KEY = os.environ.get('CAPMONSTER_API_KEY', '')
URL_MOVILIDAD = "https://webfenix.movilidadbogota.gov.co/#/consulta-pagos"

TIPO_IDENTIFICACION_MAP = {
    'CEDULA': 'C\u00e9dula de ciudadan\u00eda',
    'CEDULA_EXTRANJERIA': 'C\u00e9dula de extranjer\u00eda',
    'NIT': 'NIT',
    'PASAPORTE': 'Pasaporte',
}

MATH_OPS = {
    '+': operator.add,
    '-': operator.sub,
    '*': operator.mul,
    'x': operator.mul,
    'X': operator.mul,
    '/': operator.floordiv,
}


def log(message: str):
    """Log a stderr para no interferir con el JSON de salida."""
    print(message, file=sys.stderr)


def solve_recaptcha_v2(page) -> str:
    """
    Resuelve reCAPTCHA v2 usando CapMonster Cloud API.

    1. Busca el iframe de reCAPTCHA y extrae el sitekey.
    2. Envia la tarea a CapMonster.
    3. Hace polling hasta obtener el token.
    4. Inyecta el token en la pagina.

    Returns:
        str: Token resuelto, o None si falla.
    """
    if not CAPMONSTER_API_KEY:
        log("ERROR: CAPMONSTER_API_KEY no configurada")
        return None

    try:
        # Paso 1: Extraer sitekey del iframe de reCAPTCHA
        log("Buscando iframe de reCAPTCHA v2...")
        sitekey = None

        # Intentar extraer del iframe src
        recaptcha_iframes = page.locator("iframe[src*='recaptcha'], iframe[src*='google.com/recaptcha']")
        iframe_count = recaptcha_iframes.count()
        log(f"  Iframes de reCAPTCHA encontrados: {iframe_count}")

        for i in range(iframe_count):
            src = recaptcha_iframes.nth(i).get_attribute('src') or ''
            key_match = re.search(r'k=([A-Za-z0-9_-]+)', src)
            if key_match:
                sitekey = key_match.group(1)
                log(f"  Sitekey extraido del iframe: {sitekey}")
                break

        # Fallback: buscar en el div g-recaptcha
        if not sitekey:
            recaptcha_div = page.locator("div.g-recaptcha[data-sitekey], div[data-sitekey]")
            if recaptcha_div.count() > 0:
                sitekey = recaptcha_div.first.get_attribute('data-sitekey')
                log(f"  Sitekey extraido del div: {sitekey}")

        # Fallback: buscar en scripts de la pagina
        if not sitekey:
            sitekey = page.evaluate("""() => {
                var scripts = document.querySelectorAll('script[src*="recaptcha"]');
                for (var s of scripts) {
                    var m = s.src.match(/render=([A-Za-z0-9_-]+)/);
                    if (m) return m[1];
                }
                var divs = document.querySelectorAll('[data-sitekey]');
                for (var d of divs) {
                    if (d.dataset.sitekey) return d.dataset.sitekey;
                }
                return null;
            }""")
            if sitekey:
                log(f"  Sitekey extraido via JS: {sitekey}")

        if not sitekey:
            log("ERROR: No se pudo encontrar el sitekey de reCAPTCHA")
            return None

        # Paso 2: Crear tarea en CapMonster
        page_url = page.url
        log("Resolviendo reCAPTCHA v2 con CapMonster...")
        log(f"  Website URL: {page_url}")
        log(f"  Website Key: {sitekey}")

        create_task_url = "https://api.capmonster.cloud/createTask"
        get_result_url = "https://api.capmonster.cloud/getTaskResult"

        create_payload = {
            "clientKey": CAPMONSTER_API_KEY,
            "task": {
                "type": "RecaptchaV2TaskProxyless",
                "websiteURL": page_url,
                "websiteKey": sitekey
            }
        }

        log("Enviando tarea a CapMonster...")
        create_response = requests.post(create_task_url, json=create_payload, timeout=30)
        create_data = create_response.json()

        if create_data.get('errorId', 1) != 0:
            error_code = create_data.get('errorCode', 'UNKNOWN')
            error_desc = create_data.get('errorDescription', 'Sin descripcion')
            log(f"ERROR creando tarea: {error_code} - {error_desc}")
            return None

        task_id = create_data.get('taskId')
        if not task_id:
            log("ERROR: No se recibio taskId")
            return None

        log(f"Tarea creada. TaskId: {task_id}")

        # Paso 3: Polling para obtener el resultado
        max_attempts = 60  # 60 x 3s = 180s max (reCAPTCHA v2 tarda mas)
        for attempt in range(max_attempts):
            time.sleep(3)

            get_result_payload = {
                "clientKey": CAPMONSTER_API_KEY,
                "taskId": task_id
            }

            result_response = requests.post(get_result_url, json=get_result_payload, timeout=30)
            result_data = result_response.json()

            if result_data.get('errorId', 1) != 0:
                error_code = result_data.get('errorCode', 'UNKNOWN')
                log(f"ERROR obteniendo resultado: {error_code}")
                return None

            status = result_data.get('status')

            if status == 'ready':
                solution = result_data.get('solution', {})
                token = solution.get('gRecaptchaResponse')
                if token:
                    log(f"reCAPTCHA resuelto en {(attempt + 1) * 3}s")
                    return token
                log("ERROR: Solucion recibida pero sin gRecaptchaResponse")
                return None

            elif status == 'processing':
                if attempt % 5 == 0:
                    log(f"Procesando reCAPTCHA... ({(attempt + 1) * 3}s)")
                continue

        log("ERROR: Timeout esperando resolucion del reCAPTCHA")
        return None

    except Exception as e:
        log(f"ERROR resolviendo reCAPTCHA: {str(e)}")
        return None


def inject_recaptcha_token(page, token: str) -> bool:
    """
    Inyecta el token de reCAPTCHA en la pagina.
    Intenta multiples metodos para asegurar que el token se aplique.
    """
    try:
        log("Inyectando token de reCAPTCHA...")

        result = page.evaluate("""(token) => {
            var injected = false;

            // Metodo 1: Textarea g-recaptcha-response
            var textarea = document.querySelector('textarea[name="g-recaptcha-response"]');
            if (!textarea) {
                textarea = document.getElementById('g-recaptcha-response');
            }
            if (textarea) {
                textarea.style.display = 'block';
                textarea.value = token;
                textarea.style.display = 'none';
                injected = true;
            }

            // Metodo 2: Buscar todos los textareas de recaptcha (puede haber multiples)
            var allTextareas = document.querySelectorAll('textarea[name="g-recaptcha-response"]');
            for (var i = 0; i < allTextareas.length; i++) {
                allTextareas[i].value = token;
                injected = true;
            }

            // Metodo 3: Intentar llamar al callback de reCAPTCHA
            try {
                if (typeof ___grecaptcha_cfg !== 'undefined') {
                    var clients = ___grecaptcha_cfg.clients;
                    if (clients) {
                        for (var cid in clients) {
                            var client = clients[cid];
                            // Buscar recursivamente el callback
                            var findCallback = function(obj, depth) {
                                if (depth > 5 || !obj) return null;
                                for (var key in obj) {
                                    if (typeof obj[key] === 'function' &&
                                        key !== 'bind' && key !== 'call' && key !== 'apply') {
                                        return obj[key];
                                    }
                                    if (typeof obj[key] === 'object') {
                                        var found = findCallback(obj[key], depth + 1);
                                        if (found) return found;
                                    }
                                }
                                return null;
                            };
                            var cb = findCallback(client, 0);
                            if (cb) {
                                cb(token);
                                injected = true;
                            }
                        }
                    }
                }
            } catch (e) {}

            // Metodo 4: Intentar grecaptcha.getResponse callback
            try {
                if (window.grecaptcha && window.grecaptcha.getResponse) {
                    // Buscar data-callback en el widget
                    var widget = document.querySelector('.g-recaptcha[data-callback]');
                    if (widget) {
                        var cbName = widget.getAttribute('data-callback');
                        if (cbName && window[cbName]) {
                            window[cbName](token);
                            injected = true;
                        }
                    }
                }
            } catch (e) {}

            // Metodo 5: Angular-specific - dispatch input event on hidden textarea
            try {
                if (textarea) {
                    var event = new Event('input', { bubbles: true });
                    textarea.dispatchEvent(event);
                    var changeEvent = new Event('change', { bubbles: true });
                    textarea.dispatchEvent(changeEvent);
                }
            } catch (e) {}

            return injected;
        }""", token)

        if result:
            log("Token de reCAPTCHA inyectado correctamente")
        else:
            log("ADVERTENCIA: No se pudo confirmar la inyeccion del token")

        return result

    except Exception as e:
        log(f"ERROR inyectando token: {str(e)}")
        return False


def solve_math_captcha(page) -> bool:
    """
    Resuelve el captcha matematico de la pagina.
    Busca texto como 'Ingrese la respuesta correcta: 6 + 10' y calcula el resultado.
    """
    try:
        log("Buscando captcha matematico...")

        body_text = page.inner_text("body")

        # Patron 1: "Ingrese la respuesta correcta: N op N"
        math_match = re.search(
            r'[Ii]ngrese\s+la\s+respuesta\s+(?:correcta\s*)?:?\s*(\d+)\s*([+\-*xX/])\s*(\d+)',
            body_text
        )

        # Patron 2: "Respuesta: N op N" o similar
        if not math_match:
            math_match = re.search(
                r'[Rr]espuesta\s*:?\s*(\d+)\s*([+\-*xX/])\s*(\d+)',
                body_text
            )

        # Patron 3: texto generico con operacion matematica cerca de un input
        if not math_match:
            math_match = re.search(
                r'(\d+)\s*([+\-*xX/])\s*(\d+)\s*=\s*\?',
                body_text
            )

        # Patron 4: buscar via JS en labels o textos asociados al campo
        if not math_match:
            math_text = page.evaluate("""() => {
                var labels = document.querySelectorAll('label, span, p, div');
                for (var el of labels) {
                    var t = el.textContent || '';
                    var m = t.match(/(\\d+)\\s*([+\\-*xX\\/])\\s*(\\d+)/);
                    if (m) return t;
                }
                return null;
            }""")
            if math_text:
                math_match = re.search(r'(\d+)\s*([+\-*xX/])\s*(\d+)', math_text)

        if not math_match:
            log("ADVERTENCIA: No se encontro captcha matematico (puede no existir)")
            return True  # Puede que no haya captcha matematico

        num1 = int(math_match.group(1))
        op_char = math_match.group(2)
        num2 = int(math_match.group(3))

        op_func = MATH_OPS.get(op_char)
        if not op_func:
            log(f"ERROR: Operador desconocido: {op_char}")
            return False

        answer = op_func(num1, num2)
        log(f"  Captcha matematico: {num1} {op_char} {num2} = {answer}")

        # Buscar el input para la respuesta
        answer_input = None
        answer_selectors = [
            "input[placeholder*='espuesta']",
            "input[placeholder*='resultado']",
            "input[placeholder*='Respuesta']",
            "input[placeholder*='Resultado']",
            "input[formcontrolname*='captcha']",
            "input[formcontrolname*='respuesta']",
            "input[formcontrolname*='answer']",
            "input[id*='captcha']",
            "input[id*='respuesta']",
            "input[name*='captcha']",
            "input[name*='respuesta']",
        ]

        for selector in answer_selectors:
            try:
                locator = page.locator(selector)
                if locator.count() > 0:
                    answer_input = locator.first
                    log(f"  Input de respuesta encontrado: {selector}")
                    break
            except Exception:
                continue

        # Fallback: buscar input cerca del texto del captcha
        if not answer_input:
            answer_input_found = page.evaluate("""() => {
                var labels = document.querySelectorAll('label, span, p, div');
                for (var el of labels) {
                    var t = (el.textContent || '').toLowerCase();
                    if (t.includes('respuesta') || t.match(/\\d+\\s*[+\\-*xX\\/]\\s*\\d+/)) {
                        var parent = el.closest('div, form, fieldset');
                        if (parent) {
                            var input = parent.querySelector(
                                'input[type="text"], input[type="number"], input:not([type])'
                            );
                            if (input) {
                                input.setAttribute('data-math-captcha', 'true');
                                return true;
                            }
                        }
                    }
                }
                return false;
            }""")
            if answer_input_found:
                answer_input = page.locator("input[data-math-captcha='true']")
                log("  Input encontrado via fallback JS")

        if not answer_input or answer_input.count() == 0:
            log("ERROR: No se encontro el input para la respuesta del captcha matematico")
            return False

        # Limpiar y llenar el input
        answer_input.click()
        answer_input.fill(str(answer))
        page.wait_for_timeout(500)

        # Disparar eventos Angular
        page.evaluate("""(answer) => {
            var input = document.querySelector('input[data-math-captcha="true"]');
            if (!input) {
                var selectors = [
                    "input[placeholder*='espuesta']",
                    "input[placeholder*='Respuesta']",
                    "input[formcontrolname*='captcha']",
                    "input[formcontrolname*='respuesta']",
                    "input[id*='captcha']"
                ];
                for (var sel of selectors) {
                    input = document.querySelector(sel);
                    if (input) break;
                }
            }
            if (input) {
                input.value = answer;
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                input.dispatchEvent(new Event('blur', { bubbles: true }));
            }
        }""", str(answer))

        log(f"  Respuesta ingresada: {answer}")
        return True

    except Exception as e:
        log(f"ERROR resolviendo captcha matematico: {str(e)}")
        return False


def select_document_type(page, tipo_identificacion: str) -> bool:
    """
    Selecciona el tipo de documento en el dropdown Angular (PrimeNG / Angular Material).
    Intenta multiples estrategias para localizar y operar el dropdown.
    """
    label_text = TIPO_IDENTIFICACION_MAP.get(tipo_identificacion)
    if not label_text:
        log(f"ERROR: Tipo de identificacion no valido: {tipo_identificacion}")
        return False

    log(f"Seleccionando tipo de documento: {label_text}")

    # Estrategia 1: PrimeNG p-dropdown
    try:
        dropdown = page.locator("p-dropdown, [class*='p-dropdown']")
        if dropdown.count() > 0:
            log("  Dropdown PrimeNG encontrado")
            dropdown.first.click()
            page.wait_for_timeout(1000)

            # Buscar la opcion en el panel abierto
            option = page.locator(f"li[aria-label='{label_text}']")
            if option.count() == 0:
                option = page.locator(f"p-dropdownitem:has-text('{label_text}')")
            if option.count() == 0:
                option = page.locator(f"li:has-text('{label_text}')")
            if option.count() == 0:
                option = page.locator(f"span:has-text('{label_text}')")

            if option.count() > 0:
                option.first.click()
                page.wait_for_timeout(500)
                log(f"  Seleccionado: {label_text}")
                return True
            else:
                log("  Opcion no encontrada en PrimeNG dropdown, intentando siguiente estrategia")
    except Exception as e:
        log(f"  PrimeNG dropdown fallo: {str(e)}")

    # Estrategia 2: Angular Material mat-select
    try:
        mat_select = page.locator("mat-select, [role='combobox']")
        if mat_select.count() > 0:
            log("  mat-select encontrado")
            mat_select.first.click()
            page.wait_for_timeout(1000)

            option = page.locator(f"mat-option:has-text('{label_text}')")
            if option.count() == 0:
                option = page.locator(f"[role='option']:has-text('{label_text}')")

            if option.count() > 0:
                option.first.click()
                page.wait_for_timeout(500)
                log(f"  Seleccionado: {label_text}")
                return True
            else:
                log("  Opcion no encontrada en mat-select, cerrando overlay...")
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)
    except Exception as e:
        log(f"  mat-select fallo: {str(e)}")
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)
        except Exception:
            pass

    # Estrategia 3: select nativo HTML
    try:
        selects = page.locator("select")
        if selects.count() > 0:
            log("  Select nativo encontrado")
            for i in range(selects.count()):
                sel = selects.nth(i)
                options_text = sel.inner_text()
                if 'cedula' in options_text.lower() or 'ciudadania' in options_text.lower():
                    # Intentar seleccionar por label visible
                    sel.select_option(label=label_text)
                    page.wait_for_timeout(500)
                    log(f"  Seleccionado via select nativo: {label_text}")
                    return True
    except Exception as e:
        log(f"  Select nativo fallo: {str(e)}")

    # Estrategia 4: cualquier dropdown generico con role=listbox
    try:
        # Hacer click en cualquier elemento que parezca un dropdown de tipo documento
        dropdown_trigger = page.locator(
            "[class*='dropdown']:not(nav *), "
            "[class*='select']:not(nav *), "
            "[formcontrolname*='tipo'], "
            "[formcontrolname*='document']"
        )
        if dropdown_trigger.count() > 0:
            log("  Dropdown generico encontrado")
            dropdown_trigger.first.click()
            page.wait_for_timeout(1000)

            option = page.locator(f"text='{label_text}'")
            if option.count() > 0:
                option.first.click()
                page.wait_for_timeout(500)
                log(f"  Seleccionado via dropdown generico: {label_text}")
                return True
    except Exception as e:
        log(f"  Dropdown generico fallo: {str(e)}")

    # Estrategia 5: JS directo - buscar y clickear
    try:
        log("  Intentando seleccion via JavaScript...")
        selected = page.evaluate("""(labelText) => {
            // Buscar PrimeNG dropdown
            var pDropdowns = document.querySelectorAll('p-dropdown, .p-dropdown');
            for (var dd of pDropdowns) {
                dd.click();
            }

            // Buscar mat-select
            var matSelects = document.querySelectorAll('mat-select');
            for (var ms of matSelects) {
                ms.click();
            }

            return false;
        }""", label_text)

        if not selected:
            page.wait_for_timeout(1000)
            # Intentar click en la opcion que aparecio
            option = page.locator(f"text='{label_text}'")
            if option.count() > 0:
                option.first.click()
                page.wait_for_timeout(500)
                log(f"  Seleccionado via JS fallback: {label_text}")
                return True
    except Exception as e:
        log(f"  JS fallback fallo: {str(e)}")

    log(f"ERROR: No se pudo seleccionar el tipo de documento: {label_text}")
    return False


def enter_document_number(page, numero_identificacion: str) -> bool:
    """Ingresa el numero de documento en el campo de identificacion."""
    log(f"Ingresando numero de documento: {numero_identificacion}")

    # Intentar multiples selectores para el input
    input_selectors = [
        "input#identificacion",
        "input[id*='identificacion']",
        "input[formcontrolname*='identificacion']",
        "input[formcontrolname*='numero']",
        "input[formcontrolname*='document']",
        "input[placeholder*='identificaci']",
        "input[placeholder*='documento']",
        "input[placeholder*='numero']",
        "input[placeholder*='N\\u00famero']",
        "input[name*='identificacion']",
        "input[name*='numero']",
    ]

    for selector in input_selectors:
        try:
            locator = page.locator(selector)
            if locator.count() > 0:
                # Asegurarse de no seleccionar el campo de "placa"
                for i in range(locator.count()):
                    el = locator.nth(i)
                    placeholder = (el.get_attribute('placeholder') or '').lower()
                    name = (el.get_attribute('name') or '').lower()
                    el_id = (el.get_attribute('id') or '').lower()
                    formcontrol = (el.get_attribute('formcontrolname') or '').lower()

                    # Excluir campos de placa
                    if 'placa' in placeholder or 'placa' in name or 'placa' in el_id:
                        continue
                    if 'placa' in formcontrol:
                        continue

                    el.click()
                    el.fill(str(numero_identificacion))
                    page.wait_for_timeout(500)

                    # Disparar eventos Angular
                    page.evaluate("""(selector) => {
                        var inputs = document.querySelectorAll(selector);
                        for (var input of inputs) {
                            if (input.value) {
                                input.dispatchEvent(new Event('input', { bubbles: true }));
                                input.dispatchEvent(new Event('change', { bubbles: true }));
                                input.dispatchEvent(new Event('blur', { bubbles: true }));
                            }
                        }
                    }""", selector)

                    log(f"  Documento ingresado con selector: {selector}")
                    return True
        except Exception:
            continue

    # Fallback: buscar input de texto que no sea placa, captcha, ni recaptcha
    try:
        found = page.evaluate("""(numero) => {
            var inputs = document.querySelectorAll(
                'input[type="text"], input[type="number"], input:not([type])'
            );
            for (var input of inputs) {
                var ph = (input.placeholder || '').toLowerCase();
                var name = (input.name || '').toLowerCase();
                var id = (input.id || '').toLowerCase();
                var fc = (input.getAttribute('formcontrolname') || '').toLowerCase();

                // Excluir campos irrelevantes
                if (ph.includes('placa') || name.includes('placa') || id.includes('placa')) continue;
                if (ph.includes('respuesta') || ph.includes('captcha')) continue;
                if (input.type === 'hidden') continue;
                if (input.closest('.g-recaptcha')) continue;

                // Buscar campos de documento/identificacion
                if (id.includes('identific') || name.includes('identific') ||
                    fc.includes('identific') || fc.includes('numero') ||
                    ph.includes('identific') || ph.includes('documento') ||
                    ph.includes('numero')) {
                    input.value = numero;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                    return true;
                }
            }
            return false;
        }""", str(numero_identificacion))

        if found:
            log("  Documento ingresado via JS fallback")
            return True
    except Exception as e:
        log(f"  JS fallback fallo: {str(e)}")

    log("ERROR: No se pudo encontrar el campo de numero de documento")
    return False


def click_consultar(page) -> bool:
    """Hace click en el boton de consultar."""
    log("Buscando boton Consultar...")

    button_selectors = [
        "button:has-text('Consultar')",
        "button:has-text('CONSULTAR')",
        "button:has-text('Buscar')",
        "button:has-text('BUSCAR')",
        "button[type='submit']",
        "input[type='submit']",
        "button[class*='consultar']",
        "button[id*='consultar']",
        "a:has-text('Consultar')",
    ]

    for selector in button_selectors:
        try:
            btn = page.locator(selector)
            if btn.count() > 0:
                btn.first.click()
                log(f"  Click en boton: {selector}")
                return True
        except Exception:
            continue

    # Fallback JS
    try:
        clicked = page.evaluate("""() => {
            var buttons = document.querySelectorAll('button, input[type="submit"], a.btn');
            for (var btn of buttons) {
                var text = (btn.textContent || btn.value || '').toLowerCase().trim();
                if (text.includes('consultar') || text.includes('buscar')) {
                    btn.click();
                    return true;
                }
            }
            return false;
        }""")
        if clicked:
            log("  Click via JS fallback")
            return True
    except Exception as e:
        log(f"  JS click fallo: {str(e)}")

    log("ERROR: No se pudo encontrar el boton Consultar")
    return False


def extract_comparendos(page) -> list:
    """
    Extrae la lista de comparendos de la pagina de resultados.
    Intenta expandir detalles y extraer informacion de tablas.
    """
    comparendos = []

    try:
        # Intentar expandir todos los detalles
        log("Buscando links/botones 'Ver detalle'...")
        detail_buttons = page.locator(
            "button:has-text('Ver detalle'), "
            "a:has-text('Ver detalle'), "
            "button:has-text('Detalle'), "
            "a:has-text('Detalle'), "
            "[class*='expand'], "
            "[class*='detail']"
        )
        detail_count = detail_buttons.count()
        log(f"  Botones de detalle encontrados: {detail_count}")

        for i in range(detail_count):
            try:
                detail_buttons.nth(i).click()
                page.wait_for_timeout(1000)
            except Exception:
                continue

        page.wait_for_timeout(2000)

        # Extraer datos de tablas
        body_text = page.inner_text("body")

        # Estrategia 1: Buscar filas de tabla con datos de comparendos
        rows = page.locator("table tbody tr, tr[class*='row'], [class*='comparendo']")
        row_count = rows.count()
        log(f"  Filas de tabla encontradas: {row_count}")

        if row_count > 0:
            for i in range(row_count):
                try:
                    row = rows.nth(i)
                    row_text = row.inner_text().strip()

                    if not row_text or len(row_text) < 5:
                        continue

                    comparendo = {'index': len(comparendos) + 1}

                    cells = row.locator("td").all()

                    if cells:
                        cell_texts = []
                        for cell in cells:
                            cell_texts.append(cell.inner_text().strip())

                        # Intentar mapear celdas a campos
                        for ct in cell_texts:
                            # Numero de comparendo (10-25 digitos)
                            num_match = re.search(r'\b(\d{10,25})\b', ct)
                            if num_match and 'numero' not in comparendo:
                                comparendo['numero'] = num_match.group(1)

                            # Placa (patron ABC123 o ABC12D)
                            placa_match = re.search(r'\b([A-Z]{3}\d{2,3}[A-Z0-9]?)\b', ct.upper())
                            if placa_match and 'placa' not in comparendo:
                                comparendo['placa'] = placa_match.group(1)

                            # Fechas (dd/mm/yyyy o dd-mm-yyyy)
                            fecha_match = re.search(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b', ct)
                            if fecha_match:
                                if 'fecha_imposicion' not in comparendo:
                                    comparendo['fecha_imposicion'] = fecha_match.group(1)
                                elif 'fecha_notificacion' not in comparendo:
                                    comparendo['fecha_notificacion'] = fecha_match.group(1)

                            # Estado
                            for estado in ['VIGENTE', 'PAGADO', 'ANULADO', 'CANCELADO',
                                           'PENDIENTE', 'EN COBRO', 'PRESCRITO']:
                                if estado in ct.upper():
                                    comparendo['estado'] = estado
                                    break

                            # Montos ($ seguido de numero)
                            monto_matches = re.findall(r'\$\s*[\d.,]+', ct)
                            for monto in monto_matches:
                                if 'saldo' not in comparendo:
                                    comparendo['saldo'] = monto.strip()
                                elif 'intereses' not in comparendo:
                                    comparendo['intereses'] = monto.strip()
                                elif 'total' not in comparendo:
                                    comparendo['total'] = monto.strip()

                            # Codigo de infraccion (ej: C02, D12, H13)
                            infraccion_match = re.search(r'\b([A-Z]\d{2,3})\b', ct.upper())
                            if infraccion_match and 'infraccion' not in comparendo:
                                comparendo['infraccion'] = infraccion_match.group(1)

                    # Si no se extrajeron datos de celdas, intentar del texto completo
                    if len(comparendo) <= 1:
                        num_match = re.search(r'\b(\d{10,25})\b', row_text)
                        if num_match:
                            comparendo['numero'] = num_match.group(1)

                        placa_match = re.search(r'\b([A-Z]{3}\d{2,3}[A-Z0-9]?)\b', row_text.upper())
                        if placa_match:
                            comparendo['placa'] = placa_match.group(1)

                    # Solo agregar si tiene datos relevantes
                    if len(comparendo) > 1:
                        comparendo['texto_completo'] = row_text[:500]
                        comparendos.append(comparendo)

                except Exception as e:
                    log(f"  Error en fila {i}: {str(e)}")
                    continue

        # Estrategia 2: Si no se encontraron tablas, buscar en cards/paneles
        if not comparendos:
            log("  Intentando extraccion por cards/paneles...")
            cards = page.locator(
                "[class*='card'], [class*='panel'], [class*='accordion'], "
                "[class*='comparendo'], [class*='detail'], mat-expansion-panel, "
                "p-accordion, p-panel"
            )
            card_count = cards.count()
            log(f"  Cards/paneles encontrados: {card_count}")

            for i in range(card_count):
                try:
                    card = cards.nth(i)
                    card_text = card.inner_text().strip()

                    if not card_text or len(card_text) < 10:
                        continue

                    comparendo = {'index': len(comparendos) + 1}

                    # Extraer campos del texto del card
                    num_match = re.search(r'\b(\d{10,25})\b', card_text)
                    if num_match:
                        comparendo['numero'] = num_match.group(1)

                    placa_match = re.search(r'\b([A-Z]{3}\d{2,3}[A-Z0-9]?)\b', card_text.upper())
                    if placa_match:
                        comparendo['placa'] = placa_match.group(1)

                    fechas = re.findall(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b', card_text)
                    if len(fechas) >= 1:
                        comparendo['fecha_imposicion'] = fechas[0]
                    if len(fechas) >= 2:
                        comparendo['fecha_notificacion'] = fechas[1]

                    for estado in ['VIGENTE', 'PAGADO', 'ANULADO', 'CANCELADO',
                                   'PENDIENTE', 'EN COBRO', 'PRESCRITO']:
                        if estado in card_text.upper():
                            comparendo['estado'] = estado
                            break

                    montos = re.findall(r'\$\s*[\d.,]+', card_text)
                    if len(montos) >= 1:
                        comparendo['saldo'] = montos[0].strip()
                    if len(montos) >= 2:
                        comparendo['intereses'] = montos[1].strip()
                    if len(montos) >= 3:
                        comparendo['total'] = montos[-1].strip()

                    infraccion_match = re.search(r'\b([A-Z]\d{2,3})\b', card_text.upper())
                    if infraccion_match:
                        comparendo['infraccion'] = infraccion_match.group(1)

                    # Buscar descripcion e direccion por labels
                    desc_match = re.search(
                        r'[Dd]escripci[oó]n\s*:?\s*(.+?)(?:\n|$)', card_text
                    )
                    if desc_match:
                        comparendo['descripcion'] = desc_match.group(1).strip()[:200]

                    dir_match = re.search(
                        r'[Dd]irecci[oó]n\s*:?\s*(.+?)(?:\n|$)', card_text
                    )
                    if dir_match:
                        comparendo['direccion'] = dir_match.group(1).strip()[:200]

                    if len(comparendo) > 1:
                        comparendo['texto_completo'] = card_text[:500]
                        comparendos.append(comparendo)

                except Exception as e:
                    log(f"  Error en card {i}: {str(e)}")
                    continue

        # Estrategia 3: Extraccion global del body como ultimo recurso
        if not comparendos:
            log("  Intentando extraccion global del body...")
            all_nums = re.findall(r'\b(\d{10,25})\b', body_text)
            all_placas = re.findall(r'\b([A-Z]{3}\d{2,3}[A-Z0-9]?)\b', body_text.upper())
            all_montos = re.findall(r'\$\s*[\d.,]+', body_text)
            all_fechas = re.findall(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b', body_text)

            if all_nums or all_placas or all_montos:
                comparendo = {'index': 1}
                if all_nums:
                    comparendo['numero'] = all_nums[0]
                if all_placas:
                    comparendo['placa'] = all_placas[0]
                if all_fechas:
                    comparendo['fecha_imposicion'] = all_fechas[0]
                if all_montos:
                    comparendo['saldo'] = all_montos[0].strip()
                    if len(all_montos) > 1:
                        comparendo['total'] = all_montos[-1].strip()

                for estado in ['VIGENTE', 'PAGADO', 'ANULADO', 'CANCELADO',
                               'PENDIENTE', 'EN COBRO', 'PRESCRITO']:
                    if estado in body_text.upper():
                        comparendo['estado'] = estado
                        break

                if len(comparendo) > 1:
                    comparendo['texto_completo'] = body_text[:1000]
                    comparendos.append(comparendo)

    except Exception as e:
        log(f"ERROR extrayendo comparendos: {str(e)}")

    return comparendos


def consultar_movilidad(tipo_identificacion: str, numero_identificacion: str) -> dict:
    """Realiza la consulta completa de comparendos en Movilidad Bogota."""

    base_result = {
        'tipo_identificacion': tipo_identificacion,
        'numero_identificacion': numero_identificacion
    }

    with sync_playwright() as p:
        browser = None
        try:
            log("Iniciando navegador...")
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

            # Navegar a la pagina
            log(f"Navegando a: {URL_MOVILIDAD}")
            page.goto(URL_MOVILIDAD, wait_until='domcontentloaded')

            # Esperar 3-5 segundos para que Angular cargue
            log("Esperando carga de Angular SPA...")
            page.wait_for_timeout(5000)

            # Verificar que la pagina cargo correctamente
            page_title = page.title() or ''
            body_text = page.inner_text('body') or ''

            if '500' in page_title or 'Internal Server Error' in body_text:
                return {
                    **base_result,
                    'success': False,
                    'error': 'El sitio de Movilidad Bogota no esta disponible (Error 500)'
                }

            if '503' in page_title or 'Service Unavailable' in body_text:
                return {
                    **base_result,
                    'success': False,
                    'error': 'El sitio de Movilidad Bogota esta en mantenimiento (503)'
                }

            if 'mantenimiento' in body_text.lower():
                return {
                    **base_result,
                    'success': False,
                    'error': 'El sitio de Movilidad Bogota esta en mantenimiento'
                }

            # Paso 1: Seleccionar tipo de documento
            if not select_document_type(page, tipo_identificacion):
                return {
                    **base_result,
                    'success': False,
                    'error': 'No se pudo seleccionar el tipo de documento'
                }

            page.wait_for_timeout(1000)

            # Paso 2: Ingresar numero de documento
            if not enter_document_number(page, numero_identificacion):
                return {
                    **base_result,
                    'success': False,
                    'error': 'No se pudo ingresar el numero de documento'
                }

            page.wait_for_timeout(1000)

            # Paso 3: Resolver reCAPTCHA v2
            log("Resolviendo reCAPTCHA v2...")
            recaptcha_token = solve_recaptcha_v2(page)

            if not recaptcha_token:
                return {
                    **base_result,
                    'success': False,
                    'error': 'No se pudo resolver el reCAPTCHA v2'
                }

            if not inject_recaptcha_token(page, recaptcha_token):
                log("ADVERTENCIA: La inyeccion del token puede haber fallado, continuando...")

            page.wait_for_timeout(1000)

            # Paso 4: Resolver captcha matematico
            log("Resolviendo captcha matematico...")
            if not solve_math_captcha(page):
                return {
                    **base_result,
                    'success': False,
                    'error': 'No se pudo resolver el captcha matematico'
                }

            page.wait_for_timeout(1000)

            # Paso 5: Click en Consultar
            if not click_consultar(page):
                return {
                    **base_result,
                    'success': False,
                    'error': 'No se pudo hacer click en el boton Consultar'
                }

            # Paso 6: Esperar resultados
            log("Esperando resultados...")
            page.wait_for_timeout(5000)

            # Paso 7: Verificar si hay resultados
            body_text = page.inner_text("body").lower()

            no_results_phrases = [
                'no se encontraron registros',
                'no tiene comparendos',
                'no se encontraron comparendos',
                'no registra comparendos',
                'no se encontraron resultados',
                'sin resultados',
                'no hay comparendos',
                'no hay registros',
                'sin comparendos',
            ]

            for phrase in no_results_phrases:
                if phrase in body_text:
                    log(f"Sin resultados: '{phrase}'")
                    return {
                        **base_result,
                        'success': True,
                        'tiene_comparendos': False,
                        'total_comparendos': 0,
                        'comparendos': [],
                        'mensaje': 'No se encontraron comparendos registrados.'
                    }

            # Paso 8: Extraer comparendos
            log("Extrayendo comparendos...")
            comparendos = extract_comparendos(page)

            if not comparendos:
                # Puede que la pagina tenga resultados pero no se pudieron parsear
                # Guardar screenshot info y texto del body para debug
                raw_body = page.inner_text("body")
                has_any_data = bool(
                    re.search(r'\$\s*[\d.,]+', raw_body)
                    or re.search(r'\b\d{10,25}\b', raw_body)
                )

                if has_any_data:
                    log("ADVERTENCIA: Se detectaron datos pero no se pudieron parsear")
                    return {
                        **base_result,
                        'success': True,
                        'tiene_comparendos': True,
                        'total_comparendos': 0,
                        'comparendos': [],
                        'mensaje': 'Se detectaron posibles comparendos pero no se pudieron extraer.',
                        'body_preview': raw_body[:2000]
                    }
                else:
                    return {
                        **base_result,
                        'success': True,
                        'tiene_comparendos': False,
                        'total_comparendos': 0,
                        'comparendos': [],
                        'mensaje': 'No se encontraron comparendos registrados.'
                    }

            total = len(comparendos)
            log(f"Total comparendos encontrados: {total}")

            return {
                **base_result,
                'success': True,
                'tiene_comparendos': True,
                'total_comparendos': total,
                'comparendos': comparendos,
                'mensaje': f'Se encontraron {total} comparendo(s).'
            }

        except Exception as e:
            return {
                **base_result,
                'success': False,
                'error': f'Error inesperado: {str(e)[:300]}'
            }
        finally:
            if browser:
                browser.close()


def main():
    if len(sys.argv) < 3:
        print(json.dumps({
            'success': False,
            'error': (
                'Uso: python scrape_movilidad_codespace.py '
                '<tipo_identificacion> <numero_identificacion>\n'
                'Tipos validos: CEDULA, CEDULA_EXTRANJERIA, NIT, PASAPORTE'
            )
        }, ensure_ascii=False))
        return

    tipo_identificacion = sys.argv[1].upper()
    numero_identificacion = sys.argv[2]

    if tipo_identificacion not in TIPO_IDENTIFICACION_MAP:
        print(json.dumps({
            'success': False,
            'error': (
                f'Tipo de identificacion no valido: {tipo_identificacion}. '
                f'Tipos validos: {", ".join(TIPO_IDENTIFICACION_MAP.keys())}'
            ),
            'tipo_identificacion': tipo_identificacion,
            'numero_identificacion': numero_identificacion
        }, ensure_ascii=False))
        return

    if not CAPMONSTER_API_KEY:
        print(json.dumps({
            'success': False,
            'error': 'Variable de entorno CAPMONSTER_API_KEY no configurada',
            'tipo_identificacion': tipo_identificacion,
            'numero_identificacion': numero_identificacion
        }, ensure_ascii=False))
        return

    result = consultar_movilidad(tipo_identificacion, numero_identificacion)

    # Solo el JSON al stdout (los logs van a stderr)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
