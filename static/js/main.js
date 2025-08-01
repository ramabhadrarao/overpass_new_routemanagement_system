// Main JavaScript file for HPCL Route Management System

$(document).ready(function() {
    // Initialize tooltips
    $('[data-bs-toggle="tooltip"]').tooltip();
    
    // Initialize popovers
    $('[data-bs-toggle="popover"]').popover();
    
    // Auto-dismiss alerts after 5 seconds
    setTimeout(function() {
        $('.alert').fadeOut('slow');
    }, 5000);
    
    // Confirm before delete actions
    $('.delete-confirm').click(function(e) {
        if (!confirm('Are you sure you want to delete this?')) {
            e.preventDefault();
        }
    });
    
    // File upload validation
    $('#csv_file').change(function() {
        const file = this.files[0];
        const fileSize = file.size / 1024 / 1024; // in MB
        const validExtensions = ['csv'];
        const fileExtension = file.name.split('.').pop().toLowerCase();
        
        if (!validExtensions.includes(fileExtension)) {
            alert('Please upload a valid CSV file');
            this.value = '';
            return;
        }
        
        if (fileSize > 10) {
            alert('File size should not exceed 10MB');
            this.value = '';
            return;
        }
    });
    
    // Process route button
    $('.process-route').click(function() {
        const routeId = $(this).data('route-id');
        const button = $(this);
        
        button.prop('disabled', true);
        button.html('<span class="spinner-border spinner-border-sm"></span> Processing...');
        
        $.ajax({
            url: `/api/process_route/${routeId}`,
            method: 'POST',
            success: function(response) {
                if (response.success) {
                    showNotification('Route processing started', 'success');
                    checkRouteStatus(routeId);
                }
            },
            error: function(xhr) {
                showNotification('Error processing route', 'danger');
                button.prop('disabled', false);
                button.html('Process');
            }
        });
    });
    
    // Check route processing status
    function checkRouteStatus(routeId) {
        const interval = setInterval(function() {
            $.ajax({
                url: `/api/route_status/${routeId}`,
                method: 'GET',
                success: function(response) {
                    if (response.status === 'completed') {
                        clearInterval(interval);
                        location.reload();
                    } else if (response.status === 'failed') {
                        clearInterval(interval);
                        showNotification('Route processing failed', 'danger');
                        location.reload();
                    }
                }
            });
        }, 2000); // Check every 2 seconds
    }
    
    // Show notification
    function showNotification(message, type) {
        const alertHtml = `
            <div class="alert alert-${type} alert-dismissible fade show" role="alert">
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;
        $('.container').first().prepend(alertHtml);
        
        setTimeout(function() {
            $('.alert').fadeOut('slow');
        }, 5000);
    }
    
    // Dashboard statistics refresh
    if ($('#dashboard-stats').length > 0) {
        setInterval(function() {
            $.ajax({
                url: '/api/statistics',
                method: 'GET',
                success: function(data) {
                    updateDashboardStats(data);
                }
            });
        }, 30000); // Refresh every 30 seconds
    }
    
    // Update dashboard statistics
    function updateDashboardStats(data) {
        $('#total-routes').text(data.routes.total);
        $('#processed-routes').text(data.routes.processed);
        $('#processing-rate').text(data.routes.processing_rate.toFixed(1) + '%');
        
        // Update risk distribution chart if exists
        if (window.riskChart) {
            updateRiskChart(data.risk_distribution);
        }
    }
    
    // Route search functionality
    $('#route-search').on('keyup', function() {
        const searchTerm = $(this).val().toLowerCase();
        
        $('.route-row').each(function() {
            const routeName = $(this).find('.route-name').text().toLowerCase();
            const fromCode = $(this).find('.from-code').text().toLowerCase();
            const toCode = $(this).find('.to-code').text().toLowerCase();
            
            if (routeName.includes(searchTerm) || 
                fromCode.includes(searchTerm) || 
                toCode.includes(searchTerm)) {
                $(this).show();
            } else {
                $(this).hide();
            }
        });
    });
    
    // Export functionality
    $('#export-routes').click(function() {
        const format = $('#export-format').val();
        window.location.href = `/export/routes?format=${format}`;
    });
    
    // Bulk actions
    $('#select-all-routes').change(function() {
        $('.route-checkbox').prop('checked', $(this).prop('checked'));
        updateBulkActions();
    });
    
    $('.route-checkbox').change(function() {
        updateBulkActions();
    });
    
    function updateBulkActions() {
        const checkedCount = $('.route-checkbox:checked').length;
        if (checkedCount > 0) {
            $('#bulk-actions').show();
            $('#selected-count').text(checkedCount);
        } else {
            $('#bulk-actions').hide();
        }
    }
    
    // Bulk process routes
    $('#bulk-process').click(function() {
        const selectedRoutes = [];
        $('.route-checkbox:checked').each(function() {
            selectedRoutes.push($(this).val());
        });
        
        if (selectedRoutes.length === 0) {
            alert('Please select routes to process');
            return;
        }
        
        if (confirm(`Process ${selectedRoutes.length} routes?`)) {
            processRoutesInBatch(selectedRoutes);
        }
    });
    
    // Process routes in batch
    function processRoutesInBatch(routeIds) {
        const total = routeIds.length;
        let processed = 0;
        
        showProgressModal(total);
        
        routeIds.forEach(function(routeId, index) {
            setTimeout(function() {
                $.ajax({
                    url: `/api/process_route/${routeId}`,
                    method: 'POST',
                    success: function() {
                        processed++;
                        updateProgressModal(processed, total);
                        
                        if (processed === total) {
                            hideProgressModal();
                            location.reload();
                        }
                    },
                    error: function() {
                        processed++;
                        updateProgressModal(processed, total);
                    }
                });
            }, index * 1000); // Stagger requests by 1 second
        });
    }
    
    // Progress modal functions
    function showProgressModal(total) {
        const modalHtml = `
            <div class="modal fade" id="progressModal" data-bs-backdrop="static">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">Processing Routes</h5>
                        </div>
                        <div class="modal-body">
                            <div class="progress">
                                <div class="progress-bar progress-bar-striped progress-bar-animated" 
                                     style="width: 0%">0/${total}</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
        $('body').append(modalHtml);
        $('#progressModal').modal('show');
    }
    
    function updateProgressModal(processed, total) {
        const percentage = (processed / total * 100).toFixed(0);
        $('.progress-bar').css('width', percentage + '%');
        $('.progress-bar').text(`${processed}/${total}`);
    }
    
    function hideProgressModal() {
        $('#progressModal').modal('hide');
        setTimeout(function() {
            $('#progressModal').remove();
        }, 500);
    }
    
    // Map initialization for route detail page
    if ($('#route-map').length > 0) {
        initializeRouteMap();
    }
    
    function initializeRouteMap() {
        // This would integrate with a mapping library like Leaflet
        // For now, it's a placeholder
        console.log('Map initialization would happen here');
    }
});

// Utility functions
function formatDistance(km) {
    return km.toFixed(1) + ' km';
}

function formatDuration(minutes) {
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    
    if (hours > 0) {
        return `${hours}h ${mins}m`;
    }
    return `${mins}m`;
}

function getRiskColor(score) {
    if (score >= 8) return 'danger';
    if (score >= 6) return 'warning';
    if (score >= 4) return 'info';
    return 'success';
}

function getRiskText(score) {
    if (score >= 8) return 'CRITICAL';
    if (score >= 6) return 'HIGH';
    if (score >= 4) return 'MEDIUM';
    return 'LOW';
}