// SMART TABLE FILTERING FOR STOCK ANALYZER
window.onload = function(e) {
    let tables = document.getElementsByClassName("filterable");
    for (var i = 0; i < tables.length; i++) {
        addFilterRow(tables[i]);
    }
    
    // Initialize sorting if needed
    initializeSorting();
}

function addFilterRow(t) {
    let cols = t.rows[0].cells.length;
    var row = t.insertRow(1);

    row.setAttribute('class', 'filter-row');
    var headerRow = t.rows[0];

    for (var i = 0; i < cols; i++) {
        if (headerRow.cells[i].innerHTML !== 'Action' && 
            headerRow.cells[i].innerHTML !== 'Actions' &&
            headerRow.cells[i].innerHTML !== 'Cancel' &&
            headerRow.cells[i].innerHTML !== 'Update') {
            let size = Math.max(headerRow.cells[i].innerHTML.length, 10);
            let cell = row.insertCell(i);
            createCell(cell, size, t.getAttribute('id'), i);
        } else {
            row.insertCell(i);
        }
    }
}

// Create input element for filtering
function createCell(cell, size, tableId, colIndex) {
    var input = document.createElement('input');
    input.setAttribute('type', 'text');
    input.setAttribute('size', size);
    input.setAttribute('placeholder', 'Filter...');
    input.setAttribute('onchange', 'filterRows(\'' + tableId + '\', ' + colIndex + ', this.value)');
    input.setAttribute('onkeyup', 'filterRows(\'' + tableId + '\', ' + colIndex + ', this.value)');
    cell.appendChild(input);
}

function filterRows(tableId, col, filter) {
    let filterArray = filter.trim().split(" ");
    let table = document.getElementById(tableId);
    let rows = table.rows;

    let op = filterArray.length >= 2 ? filterArray[0] : 'NA';
    let filterVal = filterArray.length >= 2 ? filterArray[1] : filterArray[0];

    for (i = 2; i < rows.length; i++) {
        if (rows[i].cells[col]) {
            let tdText = rows[i].cells[col].textContent.trim();
            let display = false;

            if (op === 'NA' && (tdText.toLowerCase().includes(filterVal.toLowerCase()) || filterVal === '')) {
                display = true;
            } else if (op !== 'NA') {
                // Try to parse as number for comparison operations
                let tdFloatVal = parseFloat(tdText.replace(/[,%]/g, ''));
                let filterFloatVal = parseFloat(filterVal);
                
                if (!isNaN(tdFloatVal) && !isNaN(filterFloatVal)) {
                    display = (op === '>' && (tdFloatVal > filterFloatVal)) || 
                             (op === '>=' && tdFloatVal >= filterFloatVal) ||
                             (op === '<' && tdFloatVal < filterFloatVal) || 
                             (op === '<=' && tdFloatVal <= filterFloatVal) ||
                             (op === '=' && tdFloatVal === filterFloatVal) ||
                             (op === '!=' && tdFloatVal !== filterFloatVal);
                } else {
                    // Fallback to string comparison
                    display = tdText.toLowerCase().includes(filterVal.toLowerCase());
                }
            }

            rows[i].style.display = display ? '' : 'none';
        }
    }
    
    // Update visible row count
    updateRowCount(tableId);
}

function updateRowCount(tableId) {
    let table = document.getElementById(tableId);
    let rows = table.rows;
    let visibleCount = 0;
    
    for (let i = 2; i < rows.length; i++) {
        if (rows[i].style.display !== 'none') {
            visibleCount++;
        }
    }
    
    // Update or create row count display
    let countElement = document.getElementById(tableId + '-count');
    if (!countElement) {
        countElement = document.createElement('div');
        countElement.id = tableId + '-count';
        countElement.style.marginTop = '10px';
        countElement.style.fontWeight = 'bold';
        countElement.style.color = '#2c5777';
        table.parentNode.appendChild(countElement);
    }
    
    countElement.innerHTML = `Showing ${visibleCount} rows`;
}

// SORTING FUNCTIONALITY
function initializeSorting() {
    let tables = document.getElementsByClassName("filterable");
    for (let i = 0; i < tables.length; i++) {
        let table = tables[i];
        let headers = table.querySelectorAll('th');
        
        headers.forEach((header, index) => {
            if (header.innerHTML !== 'Action' && 
                header.innerHTML !== 'Actions' &&
                header.innerHTML !== 'Cancel' &&
                header.innerHTML !== 'Update') {
                header.style.cursor = 'pointer';
                header.style.userSelect = 'none';
                header.onclick = function() {
                    sortTable(table, index);
                };
                
                // Add sort indicator
                header.innerHTML += ' <span class="sort-indicator">⇅</span>';
            }
        });
    }
}

function sortTable(table, column) {
    let rows = Array.from(table.rows).slice(2); // Skip header and filter rows
    let ascending = table.getAttribute('data-sort-dir-' + column) !== 'asc';
    
    rows.sort((a, b) => {
        let aVal = a.cells[column].textContent.trim();
        let bVal = b.cells[column].textContent.trim();
        
        // Try to parse as numbers
        let aNum = parseFloat(aVal.replace(/[,%]/g, ''));
        let bNum = parseFloat(bVal.replace(/[,%]/g, ''));
        
        if (!isNaN(aNum) && !isNaN(bNum)) {
            return ascending ? aNum - bNum : bNum - aNum;
        } else {
            // String comparison
            return ascending ? 
                aVal.localeCompare(bVal) : 
                bVal.localeCompare(aVal);
        }
    });
    
    // Remove existing rows and add sorted rows
    rows.forEach(row => table.removeChild(row));
    rows.forEach(row => table.appendChild(row));
    
    // Update sort direction
    table.setAttribute('data-sort-dir-' + column, ascending ? 'asc' : 'desc');
    
    // Update sort indicators
    let headers = table.querySelectorAll('th .sort-indicator');
    headers.forEach((indicator, index) => {
        if (index === column) {
            indicator.innerHTML = ascending ? '▲' : '▼';
        } else {
            indicator.innerHTML = '⇅';
        }
    });
}

// CLEAR ALL FILTERS
function clearAllFilters(tableId) {
    let table = document.getElementById(tableId);
    let filterInputs = table.querySelectorAll('.filter-row input');
    
    filterInputs.forEach(input => {
        input.value = '';
        input.onchange();
    });
}

// EXPORT TABLE TO CSV
function exportTableToCSV(tableId, filename = 'stock_data.csv') {
    let table = document.getElementById(tableId);
    let csv = [];
    
    // Get headers (skip filter row)
    let headers = [];
    let headerRow = table.rows[0];
    for (let i = 0; i < headerRow.cells.length; i++) {
        headers.push(headerRow.cells[i].textContent.trim());
    }
    csv.push(headers.join(','));
    
    // Get visible data rows only
    for (let i = 2; i < table.rows.length; i++) {
        if (table.rows[i].style.display !== 'none') {
            let row = [];
            for (let j = 0; j < table.rows[i].cells.length; j++) {
                let cellText = table.rows[i].cells[j].textContent.trim();
                // Escape commas in data
                if (cellText.includes(',')) {
                    cellText = '"' + cellText + '"';
                }
                row.push(cellText);
            }
            csv.push(row.join(','));
        }
    }
    
    // Download CSV
    let csvContent = csv.join('\n');
    let blob = new Blob([csvContent], { type: 'text/csv' });
    let url = window.URL.createObjectURL(blob);
    let a = document.createElement('a');
    a.setAttribute('hidden', '');
    a.setAttribute('href', url);
    a.setAttribute('download', filename);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}

// UTILITY FUNCTIONS FOR STOCK ANALYZER
function formatCurrency(value) {
    return new Intl.NumberFormat('en-IN', {
        style: 'currency',
        currency: 'INR'
    }).format(value);
}

function formatPercentage(value) {
    return (value >= 0 ? '+' : '') + value.toFixed(2) + '%';
}

function highlightPercentageChanges() {
    let tables = document.getElementsByClassName("filterable");
    for (let i = 0; i < tables.length; i++) {
        let table = tables[i];
        let rows = table.rows;
        
        for (let j = 2; j < rows.length; j++) {
            let cells = rows[j].cells;
            for (let k = 0; k < cells.length; k++) {
                let cellText = cells[k].textContent.trim();
                
                // Check if it's a percentage
                if (cellText.includes('%') || 
                    (table.rows[0].cells[k] && 
                     table.rows[0].cells[k].textContent.toLowerCase().includes('change'))) {
                    let value = parseFloat(cellText.replace(/[%,]/g, ''));
                    if (!isNaN(value)) {
                        if (value > 0) {
                            cells[k].classList.add('positive-change');
                        } else if (value < 0) {
                            cells[k].classList.add('negative-change');
                        } else {
                            cells[k].classList.add('neutral-change');
                        }
                    }
                }
            }
        }
    }
}

// Initialize highlighting after page load
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(highlightPercentageChanges, 100);
});