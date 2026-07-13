"""Strict DOM scripts for Grafana matrix-variable fallback discovery."""


def open_variable_script() -> str:
    return """
    (name) => {
      const normalize = (value) => (value || '').trim().toLowerCase();
      const candidates = Array.from(document.querySelectorAll('button, [role="button"]'));
      const expected = normalize(name);
      const exactIdentities = new Set([
        expected,
        `dashboard variable ${expected}`,
        `template-variable-${expected}`,
        `variable-${expected}`,
      ]);
      const matches = candidates.filter((element) => {
        if (element.offsetParent === null) return false;
        const identities = ['data-variable', 'name', 'aria-label', 'data-testid']
          .map((attribute) => normalize(element.getAttribute(attribute)));
        const labelledBy = element.getAttribute('aria-labelledby');
        const label = labelledBy ? document.getElementById(labelledBy) : null;
        if (label) identities.push(normalize(label.textContent));
        const containerLabel = element.closest('[data-variable]');
        if (containerLabel) identities.push(normalize(containerLabel.getAttribute('data-variable')));
        const wrapper = element.closest('[data-testid*="template-variable"]');
        const wrapperLabel = wrapper && wrapper.querySelector('label, [data-testid*="label"]');
        if (wrapperLabel) identities.push(normalize(wrapperLabel.textContent));
        return identities.some((identity) => exactIdentities.has(identity));
      });
      if (matches.length !== 1) return null;
      const target = matches[0];
      target.click();
      return {
        popupId: target.getAttribute('aria-controls') || '',
        controlId: target.id || '',
      };
    }
    """


def read_variable_options_script() -> str:
    return """
    (scope) => {
      const control = scope.controlId ? document.getElementById(scope.controlId) : null;
      const popupId = (control && control.getAttribute('aria-controls')) || scope.popupId;
      const roots = [];
      if (popupId) {
        const popup = document.getElementById(popupId);
        if (popup && popup.offsetParent !== null) roots.push(popup);
      }
      if (!roots.length && scope.controlId) {
        roots.push(...document.querySelectorAll(
          `[role="listbox"][aria-labelledby="${CSS.escape(scope.controlId)}"], ` +
          `[role="menu"][aria-labelledby="${CSS.escape(scope.controlId)}"]`
        ));
      }
      if (roots.length !== 1) return [];
      return Array.from(roots[0].querySelectorAll(
        '[role="option"], [role="menuitem"], [data-testid*="variable-option"]'
      ))
        .filter((element) => element.offsetParent !== null)
        .map((element) => (element.textContent || '').trim())
        .filter(Boolean);
    }
    """
