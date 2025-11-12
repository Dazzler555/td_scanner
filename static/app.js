class TeamDriveExplorer {
    constructor() {
        this.currentTeamDrive = null;
        this.currentParent = null;
        this.currentPage = 0;
        this.pageSize = 100;
        this.breadcrumbs = [];
        this.teamDrives = [];
        this.contextMenu = document.getElementById('contextMenu');
        this.contextTarget = null;

        this.init();
    }

    async init() {
        await this.loadTeamDrives();
        this.setupEventListeners();
        this.setupContextMenu();
    }

    async loadTeamDrives() {
        try {
            const response = await fetch('/api/teamdrives');
            this.teamDrives = await response.json();
            this.renderTeamDrives();
        } catch (error) {
            console.error('Failed to load team drives:', error);
        }
    }

    renderTeamDrives() {
        const container = document.getElementById('teamDriveList');
        container.innerHTML = '';

        this.teamDrives.forEach(td => {
            const item = document.createElement('div');
            item.className = 'teamdrive-item';
            item.textContent = td.name;
            item.dataset.id = td.id;
            item.dataset.name = td.name;

            item.addEventListener('click', () => {
                this.selectTeamDrive(td.id, td.name);
            });

            container.appendChild(item);
        });
    }

    selectTeamDrive(id, name) {
        this.currentTeamDrive = id;
        this.currentParent = id;
        this.currentPage = 0;
        this.breadcrumbs = [{ id: id, name: name }];

        document.querySelectorAll('.teamdrive-item').forEach(item => {
            item.classList.toggle('active', item.dataset.id === id);
        });

        this.loadFiles();
        this.loadStats();
    }

    async loadFiles(query = '') {
        const fileList = document.getElementById('fileList');
        fileList.innerHTML = '<div class="loading">⏳ Loading...</div>';

        try {
            const params = new URLSearchParams({
                teamdrive: this.currentTeamDrive || '',
                parent: this.currentParent || '',
                q: query,
                limit: this.pageSize,
                offset: this.currentPage * this.pageSize
            });

            const response = await fetch(`/api/search?${params}`);
            const data = await response.json();

            this.renderFiles(data.files);
            this.renderPagination(data.total_count);
            this.renderBreadcrumbs();
        } catch (error) {
            console.error('Failed to load files:', error);
            fileList.innerHTML = '<div class="loading">❌ Error loading files</div>';
        }
    }

    renderFiles(files) {
        const fileList = document.getElementById('fileList');
        fileList.innerHTML = '';

        if (files.length === 0) {
            fileList.innerHTML = '<div class="loading">📭 No files found</div>';
            return;
        }

        files.forEach(file => {
            const item = document.createElement('div');
            item.className = 'file-item';
            item.dataset.id = file.id;
            item.dataset.name = file.name;
            item.dataset.path = file.path || file.name;

            const icon = document.createElement('div');
            icon.className = `file-icon ${file.is_folder ? 'folder' : 'file'}`;
            icon.textContent = file.is_folder ? '📁' : '📄';

            const name = document.createElement('div');
            name.className = 'file-name';
            name.textContent = this.truncateName(file.name, 80);
            name.title = file.name;

            const size = document.createElement('div');
            size.className = 'file-size';
            size.textContent = this.formatBytes(file.total_size || file.size);

            const date = document.createElement('div');
            date.className = 'file-date';
            date.textContent = this.formatDate(file.modified_time);

            item.appendChild(icon);
            item.appendChild(name);
            item.appendChild(size);
            item.appendChild(date);

            if (file.is_folder) {
                item.addEventListener('click', () => {
                    this.openFolder(file.id, file.name);
                });
            }

            item.addEventListener('contextmenu', (e) => {
                e.preventDefault();
                this.showContextMenu(e, file);
            });

            let pressTimer;
            item.addEventListener('touchstart', (e) => {
                pressTimer = setTimeout(() => {
                    this.showContextMenu(e.touches[0], file);
                }, 500);
            });

            item.addEventListener('touchend', () => {
                clearTimeout(pressTimer);
            });

            item.addEventListener('touchmove', () => {
                clearTimeout(pressTimer);
            });

            fileList.appendChild(item);
        });
    }

    openFolder(id, name) {
        this.currentParent = id;
        this.currentPage = 0;
        this.breadcrumbs.push({ id, name });
        this.loadFiles();
    }

    renderBreadcrumbs() {
        const breadcrumb = document.getElementById('breadcrumb');
        breadcrumb.innerHTML = '';

        this.breadcrumbs.forEach((crumb, index) => {
            const item = document.createElement('span');
            item.className = 'breadcrumb-item';
            item.textContent = this.truncateName(crumb.name, 30);
            item.title = crumb.name;

            item.addEventListener('click', () => {
                this.breadcrumbs = this.breadcrumbs.slice(0, index + 1);
                this.currentParent = crumb.id;
                this.currentPage = 0;
                this.loadFiles();
            });

            breadcrumb.appendChild(item);

            if (index < this.breadcrumbs.length - 1) {
                const separator = document.createElement('span');
                separator.className = 'breadcrumb-separator';
                separator.textContent = ' / ';
                breadcrumb.appendChild(separator);
            }
        });
    }

    async loadStats() {
        if (!this.currentTeamDrive) return;

        try {
            const response = await fetch(`/api/stats/${this.currentTeamDrive}`);
            const stats = await response.json();

            const container = document.getElementById('stats');
            container.innerHTML = `
                <div class="stat-card">
                    <h4>Total Files</h4>
                    <div class="value">${this.formatNumber(stats.total_files)}</div>
                </div>
                <div class="stat-card">
                    <h4>Total Folders</h4>
                    <div class="value">${this.formatNumber(stats.total_folders)}</div>
                </div>
                <div class="stat-card">
                    <h4>Total Size</h4>
                    <div class="value">${stats.total_size_human}</div>
                </div>
            `;
        } catch (error) {
            console.error('Failed to load stats:', error);
        }
    }

    renderPagination(totalCount) {
        const pagination = document.getElementById('pagination');
        pagination.innerHTML = '';

        const totalPages = Math.ceil(totalCount / this.pageSize);

        if (totalPages <= 1) return;

        const prevBtn = document.createElement('button');
        prevBtn.textContent = '← Previous';
        prevBtn.disabled = this.currentPage === 0;
        prevBtn.addEventListener('click', () => {
            this.currentPage--;
            this.loadFiles();
        });
        pagination.appendChild(prevBtn);

        const pageInfo = document.createElement('span');
        pageInfo.textContent = `Page ${this.currentPage + 1} of ${totalPages}`;
        pageInfo.style.padding = '0.75rem 1.25rem';
        pageInfo.style.fontWeight = '500';
        pagination.appendChild(pageInfo);

        const nextBtn = document.createElement('button');
        nextBtn.textContent = 'Next →';
        nextBtn.disabled = this.currentPage >= totalPages - 1;
        nextBtn.addEventListener('click', () => {
            this.currentPage++;
            this.loadFiles();
        });
        pagination.appendChild(nextBtn);
    }

    setupEventListeners() {
        const searchInput = document.getElementById('searchInput');
        const searchBtn = document.getElementById('searchBtn');

        const performSearch = () => {
            const query = searchInput.value.trim();
            this.currentPage = 0;
            this.loadFiles(query);
        };

        searchBtn.addEventListener('click', performSearch);
        searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                performSearch();
            }
        });

        document.addEventListener('click', () => {
            this.contextMenu.style.display = 'none';
        });
    }

    setupContextMenu() {
        const menuItems = this.contextMenu.querySelectorAll('.context-menu-item');

        menuItems.forEach(item => {
            item.addEventListener('click', (e) => {
                e.stopPropagation();
                const action = item.dataset.action;

                if (action === 'copy' && this.contextTarget) {
                    this.copyToClipboard(this.contextTarget.name);
                } else if (action === 'copy-path' && this.contextTarget) {
                    this.copyToClipboard(this.contextTarget.path);
                }

                this.contextMenu.style.display = 'none';
            });
        });
    }

    showContextMenu(event, file) {
        this.contextTarget = file;
        this.contextMenu.style.display = 'block';
        this.contextMenu.style.left = `${event.pageX || event.clientX}px`;
        this.contextMenu.style.top = `${event.pageY || event.clientY}px`;
    }

    copyToClipboard(text) {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(() => {
                console.log('✅ Copied to clipboard:', text);
            });
        } else {
            const textarea = document.createElement('textarea');
            textarea.value = text;
            textarea.style.position = 'fixed';
            textarea.style.opacity = '0';
            document.body.appendChild(textarea);
            textarea.select();
            document.execCommand('copy');
            document.body.removeChild(textarea);
        }
    }

    truncateName(name, maxLength) {
        if (name.length <= maxLength) return name;
        const ext = name.split('.').pop();
        const nameWithoutExt = name.substring(0, name.lastIndexOf('.'));
        if (nameWithoutExt.length === 0) return name.substring(0, maxLength) + '...';
        const truncated = nameWithoutExt.substring(0, maxLength - ext.length - 4) + '...' + ext;
        return truncated;
    }

    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    formatDate(dateString) {
        if (!dateString) return '';
        const date = new Date(dateString);
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
    }

    formatNumber(num) {
        return num.toLocaleString();
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new TeamDriveExplorer();
});
