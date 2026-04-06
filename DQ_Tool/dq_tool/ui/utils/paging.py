"""
Pagination utility for displaying large lists in Streamlit.
"""

import streamlit as st
import math


def paginate(items, page_size=20, key="default"):
    """
    Paginate a list of items.
    Returns: (current_page_items, total_pages, current_page_number)
    
    Args:
        items: List to paginate
        page_size: Items per page
        key: Unique identifier for this paginated list (e.g., "rules", "sessions")
    
    Stores page number in session_state[_pagination_page_{key}]
    """
    
    if not items:
        return [], 0, 0
    
    total_items = len(items)
    total_pages = math.ceil(total_items / page_size)
    
    page_key = f"_pagination_page_{key}"
    
    if page_key not in st.session_state:
        st.session_state[page_key] = 0
    
    current_page = st.session_state[page_key]
    
    # Ensure current page is valid
    if current_page >= total_pages and total_pages > 0:
        current_page = total_pages - 1
        st.session_state[page_key] = current_page
    
    # Calculate slice indices
    start_idx = current_page * page_size
    end_idx = start_idx + page_size
    
    page_items = items[start_idx:end_idx]
    
    return page_items, total_pages, current_page


def render_pagination_controls(items, page_size=20, key="default", container=None):
    """
    Render pagination controls (previous/next buttons and page info).
    
    Args:
        items: List of items being paginated
        page_size: Number of items per page
        key: Unique identifier for this paginated list (e.g., "rules", "sessions")
        container: Optional Streamlit container to render in (e.g., st.columns()[0])
    
    Returns:
        (current_page_items, total_pages, current_page_number)
    """
    
    if not items:
        return [], 0, 0
    
    total_items = len(items)
    total_pages = math.ceil(total_items / page_size)
    
    page_key = f"_pagination_page_{key}"
    
    if page_key not in st.session_state:
        st.session_state[page_key] = 0
    
    current_page = st.session_state[page_key]
    
    if current_page >= total_pages and total_pages > 0:
        current_page = total_pages - 1
        st.session_state[page_key] = current_page
    
    # Render controls
    render_target = container if container is not None else st
    
    col1, col2, col3, col4 = render_target.columns([1, 1, 2, 1])
    
    with col1:
        if st.button("⬅️ Previous", use_container_width=True, key=f"{page_key}_prev"):
            if current_page > 0:
                st.session_state[page_key] -= 1
                st.rerun()
    
    with col2:
        if st.button("Next ➡️", use_container_width=True, key=f"{page_key}_next"):
            if current_page < total_pages - 1:
                st.session_state[page_key] += 1
                st.rerun()
    
    with col3:
        st.write(f"Page **{current_page + 1}** of **{total_pages}** | Total: **{total_items}** items")
    
    with col4:
        if total_pages > 1:
            page_input = st.number_input(
                "Go to page",
                min_value=1,
                max_value=total_pages,
                value=current_page + 1,
                key=f"{page_key}_input"
            )
            if page_input != current_page + 1:
                st.session_state[page_key] = page_input - 1
                st.rerun()
    
    # Calculate slice indices
    start_idx = current_page * page_size
    end_idx = start_idx + page_size
    page_items = items[start_idx:end_idx]
    
    return page_items, total_pages, current_page


