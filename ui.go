package main

import (
	"fmt"
	"os"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

type model struct {
	table table.Model
}

type newRowsMsg []table.Row

func (m model) Init() tea.Cmd { return nil }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case newRowsMsg:
		m.table.SetRows(msg)
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			if m.table.Focused() {
				m.table.Blur()
			} else {
				m.table.Focus()
			}
		case "q", "ctrl+c":
			return m, tea.Quit
		case "enter":
			return m, tea.Batch(
				tea.Printf("Let's go to %s!", m.table.SelectedRow()[1]),
			)
		}
	}
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m model) View() string {
	return baseStyle.Render(m.table.View()) + "\n"
}

func ui() {
	columns := []table.Column{
		{Title: "City", Width: 10},
		{Title: "Country", Width: 10},
		{Title: "Population", Width: 10},
	}

	rows := []table.Row{
		{"Tokyo", "Japan", "37,274,000"},
		{"Delhi", "India", "32,065,760"},
		{"Shanghai", "China", "28,516,904"},
		{"Dhaka", "Bangladesh", "22,478,116"},
		{"São Paulo", "Brazil", "22,429,800"},
		{"Mexico City", "Mexico", "22,085,140"},
		{"Cairo", "Egypt", "21,750,020"},
		{"Beijing", "China", "21,333,332"},
		{"Mumbai", "India", "20,961,472"},
		{"Osaka", "Japan", "19,059,856"},
		{"Chongqing", "China", "16,874,740"},
		{"Karachi", "Pakistan", "16,839,950"},
		{"Istanbul", "Turkey", "15,636,243"},
		{"Kinshasa", "DR Congo", "15,628,085"},
		{"Lagos", "Nigeria", "15,387,639"},
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(8),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(s)

	m := model{t}
	p := tea.NewProgram(m)
	go func() {
		nt := newRowsMsg{
			{"Tokyoz", "Japan", "37,000"},
			{"Delhi", "India", "32,065,760"},
			{"Melbourne", "Australia", "5,150,766"},
			{"Shantou", "China", "4,490,411"},
			{"Xinbei", "Taiwan", "4,470,672"},
			{"Kabul", "Afghanistan", "4,457,882"},
			{"Ningbo", "China", "4,405,292"},
			{"Tel Aviv", "Israel", "4,343,584"},
			{"Yaounde", "Cameroon", "4,336,670"},
			{"Rome", "Italy", "4,297,877"},
			{"Shijiazhuang", "China", "4,285,135"},
			{"Montreal", "Canada", "4,276,526"},
		}
		time.Sleep(1 * time.Second)
		m.table.SetRows(nt)
		p.Send(nt)
	}()
	// p.Send send messages when data changes
	if err := p.Start(); err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}
}
