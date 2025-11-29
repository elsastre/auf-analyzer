import { useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { NavLink, Route, Routes, Navigate, useNavigate } from "react-router-dom";
import "./App.css";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

async function fetchJson(endpoint) {
  const res = await fetch(`${API_BASE}${endpoint}`);
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    throw new Error(detail.detail || `Error ${res.status}`);
  }
  return res.json();
}

const StageTab = ({ code, label, active, onClick }) => (
  <button className={`tab ${active ? "active" : ""}`} onClick={() => onClick(code)}>
    {label}
  </button>
);

const Logo = ({ logoKey, alt }) => (
  <img src={`/logos/${logoKey}.png`} alt={alt} className="team-logo" />
);

const iso2ToFlag = (code) => {
  const normalized = (code || "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(normalized)) return "";
  const base = 0x1f1e6;
  const offset = "A".charCodeAt(0);
  return String.fromCodePoint(
    base + (normalized.charCodeAt(0) - offset),
    base + (normalized.charCodeAt(1) - offset)
  );
};

function useTableData(season, stage) {
  const [state, setState] = useState({ rows: [], loading: true, error: "" });
  useEffect(() => {
    if (!season || !stage) return;
    setState((prev) => ({ ...prev, loading: true, error: "" }));
    fetchJson(`/tables?season=${season}&stage=${stage}`)
      .then((data) => setState({ rows: data.rows || [], loading: false, error: "" }))
      .catch((err) => setState({ rows: [], loading: false, error: err.message }));
  }, [season, stage]);
  return state;
}

function useFixtures(season, stage, teamId, roundFilter) {
  const [state, setState] = useState({ items: [], loading: true, error: "" });
  useEffect(() => {
    if (!season || !stage) return;
    setState((prev) => ({ ...prev, loading: true, error: "" }));
    const params = new URLSearchParams({ season, stage });
    if (teamId) params.append("team_id", teamId);
    if (roundFilter) params.append("round", roundFilter);
    fetchJson(`/fixtures?${params.toString()}`)
      .then((data) => setState({ items: data.fixtures || [], loading: false, error: "" }))
      .catch((err) => setState({ items: [], loading: false, error: err.message }));
  }, [season, stage, teamId, roundFilter]);
  return state;
}

function useScorers(season, stage) {
  const [state, setState] = useState({ items: [], loading: true, error: "" });
  useEffect(() => {
    if (!season || !stage) return;
    setState((prev) => ({ ...prev, loading: true, error: "" }));
    fetchJson(`/scorers?season=${season}&stage=${stage}`)
      .then((data) => setState({ items: data.scorers || [], loading: false, error: "" }))
      .catch((err) => setState({ items: [], loading: false, error: err.message }));
  }, [season, stage]);
  return state;
}

function useInsights(season, stage) {
  const [state, setState] = useState({ data: null, loading: true, error: "" });
  useEffect(() => {
    if (!season || !stage) return;
    setState((prev) => ({ ...prev, loading: true, error: "" }));
    fetchJson(`/stats/insights?season=${season}&stage=${stage}`)
      .then((data) => setState({ data, loading: false, error: "" }))
      .catch((err) => setState({ data: null, loading: false, error: err.message }));
  }, [season, stage]);
  return state;
}

function usePlayerStats(season, stage, teamId) {
  const [state, setState] = useState({ rows: [], loading: true, error: "" });
  useEffect(() => {
    if (!season || !stage) return;
    setState((prev) => ({ ...prev, loading: true, error: "" }));
    const params = new URLSearchParams({ season, stage });
    if (teamId) params.append("team_id", teamId);
    fetchJson(`/players?${params.toString()}`)
      .then((data) => setState({ rows: data.players || [], loading: false, error: "" }))
      .catch((err) => setState({ rows: [], loading: false, error: err.message }));
  }, [season, stage, teamId]);
  return state;
}

function Header({ seasons, stages, season, stage, onSeasonChange, onStageChange }) {
  // Capitalizar las opciones de stage
  const capitalizedStages = stages.map(s =>
    s.charAt(0).toUpperCase() + s.slice(1)
  );

  return (
    <header className="app-header">
      <div>
        <h1>AUF Analyzer – Demo offline</h1>
        <p className="muted">
          Datos persistidos en SQLite seed. Elegí temporada y torneo para navegar tablas, fixtures, goleadores y métricas al
          estilo FBref.
        </p>
      </div>
      <div className="selectors">
        <Selector
          label="Season"
          value={season ?? ""}
          options={seasons.map((s) => ({ value: s, label: s }))}
          onChange={(v) => onSeasonChange(Number(v))}
        />
        <Selector
          label="Stage"
          value={stage ?? ""}
          options={stages.map((s) => ({
            value: s,
            label: s.charAt(0).toUpperCase() + s.slice(1)
          }))}
          onChange={(v) => onStageChange(v)}
        />
      </div>
    </header>
  );
}

function Navigation() {
  const links = [
    { to: "/tables", label: "Tablas" },
    { to: "/fixtures", label: "Fixtures" },
    { to: "/scorers", label: "Goleadores" },
    { to: "/stats", label: "Gráficos" },
    { to: "/players", label: "Jugadores" },
    // { to: "/ai-consultor", label: "AI Consultor" },
    { to: "/consultas-libres", label: "Consultas IA" },
  ];
  return (
    <nav className="nav">
      {links.map((link) => (
        <NavLink key={link.to} to={link.to} className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}>
          {link.label}
        </NavLink>
      ))}
    </nav>
  );
}

function TablesPage({ season, stage, onStageChange }) {
  const table = useTableData(season, stage);
  const stageTabs = [
    { code: "apertura", label: "Apertura" },
    { code: "clausura", label: "Clausura" },
    { code: "intermedio", label: "Intermedio" },
    { code: "anual", label: "Anual" },
  ];

  return (
    <section className="section">
      <SectionHeader title="Tablas" subtitle="Tabla estilo FBref" />
      <div className="tabs">
        {stageTabs.map((tab) => (
          <StageTab key={tab.code} {...tab} active={stage === tab.code} onClick={onStageChange} />
        ))}
      </div>
      {table.loading && <p>Cargando tabla...</p>}
      {table.error && <p className="error">{table.error}</p>}
      {!table.loading && table.rows.length > 0 && <StandingsTable rows={table.rows} />}
    </section>
  );
}

function LastFive({ value }) {
  const outcomes = value ? value.split("").slice(0, 5) : [];
  while (outcomes.length < 5) outcomes.push("");
  const meta = {
    W: { label: "✔", title: "Win", tone: "win" },
    D: { label: "–", title: "Draw", tone: "draw" },
    L: { label: "✕", title: "Loss", tone: "loss" },
    "": { label: "", title: "Sin dato", tone: "empty" },
  };
  return (
    <div className="last5">
      {outcomes.map((res, idx) => {
        const info = meta[res] ?? meta[""];
        return (
          <span key={`${res}-${idx}`} className={`pill ${info.tone}`} title={info.title}>
            {info.label}
          </span>
        );
      })}
    </div>
  );
}

function StandingsTable({ rows }) {
  return (
    <div className="table-wrapper">
      <table className="data-table standings-table">
        <thead>
          <tr>
            <th className="col-n">#</th>
            <th className="col-team">Equipo</th>
            <th>MP</th>
            <th>W</th>
            <th>D</th>
            <th>L</th>
            <th>GF</th>
            <th>GA</th>
            <th>GD</th>
            <th>Pts</th>
            <th>Pts/MP</th>
            <th>Last 5</th>
            <th>Asistencia</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.team_id}>
              <td>{row.pos}</td>
              <td className="team-cell">
                {row.logo_key && <Logo logoKey={row.logo_key} alt={row.team} />}
                <span className="team-name" title={row.team}>{row.team}</span>
              </td>
              <td>{row.mp}</td>
              <td>{row.w}</td>
              <td>{row.d}</td>
              <td>{row.l}</td>
              <td>{row.gf}</td>
              <td>{row.ga}</td>
              <td>{row.gd}</td>
              <td>{row.pts}</td>
              <td>{row.ppg}</td>
              <td>
                <LastFive value={row.last5} />
              </td>
              <td>{row.avg_attendance ? Math.round(row.avg_attendance).toLocaleString("es-UY") : "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FixturesPage({ season, stage, teams }) {
  const [fixtureStage, setFixtureStage] = useState("apertura");
  const [teamFilter, setTeamFilter] = useState("");
  const [roundFilter, setRoundFilter] = useState("");
  useEffect(() => {
    setRoundFilter("");
    if (stage === "anual") {
      setFixtureStage((prev) => (prev === "clausura" ? "clausura" : "apertura"));
    } else {
      setFixtureStage(stage);
    }
  }, [stage, season]);

  const effectiveStage = stage === "anual" ? fixtureStage : stage;
  const roundCount =
    effectiveStage === "intermedio" ? 7 : effectiveStage === "apertura" || effectiveStage === "clausura" ? 15 : 0;
  const roundOptions = useMemo(
    () => [
      { value: "", label: "Todas" },
      ...Array.from({ length: roundCount }, (_, i) => ({ value: `${i + 1}`, label: `Ronda ${i + 1}` })),
    ],
    [roundCount]
  );
  const fixtures = useFixtures(season, effectiveStage, teamFilter || undefined, roundFilter || undefined);
  return (
    <section className="section">
      <SectionHeader
        title="Fixtures"
        subtitle="Calendario con asistencia, estadio y árbitro"
        extra={
          <div className="selector-row">
            <Selector
              label="Equipo"
              value={teamFilter}
              options={[{ value: "", label: "Todos" }, ...teams.map((t) => ({ value: t.id, label: t.name }))]}
              onChange={setTeamFilter}
            />
            {stage === "anual" && (
              <Selector
                label="Torneo"
                value={fixtureStage}
                options={[
                  { value: "apertura", label: "Apertura" },
                  { value: "clausura", label: "Clausura" },
                ]}
                onChange={(value) => {
                  setFixtureStage(value);
                  setRoundFilter("");
                }}
              />
            )}
            {roundCount > 0 && (
              <Selector label="Ronda" value={roundFilter} options={roundOptions} onChange={setRoundFilter} />
            )}
          </div>
        }
      />
      {stage === "anual" && (
        <p className="muted">Tabla anual no tiene fixture propio. Elegí Apertura o Clausura para navegar partidos.</p>
      )}
      {fixtures.loading && <p>Cargando fixtures...</p>}
      {fixtures.error && <p className="error">{fixtures.error}</p>}
      {!fixtures.loading && fixtures.items.length > 0 && <FixturesList fixtures={fixtures.items} />}
    </section>
  );
}

function FixturesList({ fixtures }) {
  return (
    <div style={{
      overflowX: 'auto',
      marginTop: '1rem',
      border: '1px solid #374151',
      borderRadius: '8px',
      background: '#0f172a'
    }}>
      <table style={{
        width: '100%',
        borderCollapse: 'collapse',
        minWidth: '1000px',
        fontFamily: 'inherit'
      }}>
        <thead>
          <tr style={{ background: '#1e293b' }}>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '100px', textAlign: 'left', fontSize: '0.9rem', fontWeight: '600' }}>Fecha</th>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '70px', textAlign: 'left', fontSize: '0.9rem', fontWeight: '600' }}>Hora</th>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '70px', textAlign: 'left', fontSize: '0.9rem', fontWeight: '600' }}>Ronda</th>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '180px', textAlign: 'left', fontSize: '0.9rem', fontWeight: '600' }}>Local</th>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '180px', textAlign: 'left', fontSize: '0.9rem', fontWeight: '600' }}>Visitante</th>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '90px', textAlign: 'center', fontSize: '0.9rem', fontWeight: '600' }}>Resultado</th>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '110px', textAlign: 'right', fontSize: '0.9rem', fontWeight: '600' }}>Asistencia</th>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '150px', textAlign: 'left', fontSize: '0.9rem', fontWeight: '600' }}>Estadio</th>
            <th style={{ padding: '12px 8px', border: '1px solid #374151', width: '150px', textAlign: 'left', fontSize: '0.9rem', fontWeight: '600' }}>Árbitro</th>
          </tr>
        </thead>
        <tbody>
          {fixtures.map((fx, index) => {
            const homeName = fx.home ?? fx.home_team ?? fx.home_name ?? "Local";
            const awayName = fx.away ?? fx.away_team ?? fx.away_name ?? "Visitante";

            const homeLogo = fx.home_logo_key ?? fx.home_team_logo_key ?? fx.home_logo;
            const awayLogo = fx.away_logo_key ?? fx.away_team_logo_key ?? fx.away_logo;

            let attendanceDisplay = "-";
            if (fx.attendance !== null && fx.attendance !== undefined) {
              const num = typeof fx.attendance === 'number' ? fx.attendance : parseInt(String(fx.attendance).replace(/[.,]/g, ''));
              if (!isNaN(num)) {
                attendanceDisplay = num.toLocaleString('es-UY');
              }
            }

            const rowStyle = {
              borderBottom: '1px solid #374151',
              backgroundColor: index % 2 === 0 ? '#0f172a' : 'rgba(255,255,255,0.02)'
            };

            return (
              <tr key={fx.match_id || fx.id || `fixture-${index}`} style={rowStyle}>
                {/* FECHA */}
                <td style={{ padding: '10px 8px', fontFamily: 'Monaco, Menlo, monospace', fontSize: '0.85rem', verticalAlign: 'middle' }}>
                  {fx.date || "-"}
                </td>

                {/* HORA */}
                <td style={{ padding: '10px 8px', fontFamily: 'Monaco, Menlo, monospace', fontSize: '0.85rem', verticalAlign: 'middle' }}>
                  {fx.time || "-"}
                </td>

                {/* RONDA */}
                <td style={{ padding: '10px 8px', fontFamily: 'Monaco, Menlo, monospace', fontSize: '0.85rem', verticalAlign: 'middle' }}>
                  {fx.round || "-"}
                </td>

                {/* LOCAL - SOLO UN EQUIPO */}
                <td style={{ padding: '10px 8px', verticalAlign: 'middle', width: '180px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0 }}>
                    {homeLogo && (
                      <img
                        src={`/logos/${homeLogo}.png`}
                        alt={homeName}
                        style={{ width: '22px', height: '22px', objectFit: 'contain', flexShrink: 0 }}
                      />
                    )}
                    <span style={{
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      flex: 1,
                      minWidth: 0
                    }}>
                      {homeName}
                    </span>
                  </div>
                </td>

                {/* VISITANTE - SOLO UN EQUIPO */}
                <td style={{ padding: '10px 8px', verticalAlign: 'middle', width: '180px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0 }}>
                    {awayLogo && (
                      <img
                        src={`/logos/${awayLogo}.png`}
                        alt={awayName}
                        style={{ width: '22px', height: '22px', objectFit: 'contain', flexShrink: 0 }}
                      />
                    )}
                    <span style={{
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      flex: 1,
                      minWidth: 0
                    }}>
                      {awayName}
                    </span>
                  </div>
                </td>

                {/* RESULTADO - SOLO EL SCORE */}
                <td style={{
                  padding: '10px 8px',
                  textAlign: 'center',
                  fontFamily: 'Monaco, Menlo, monospace',
                  fontSize: '0.85rem',
                  fontWeight: '600',
                  verticalAlign: 'middle',
                  background: 'rgba(255,255,255,0.05)'
                }}>
                  {fx.home_goals != null ? fx.home_goals : "-"} - {fx.away_goals != null ? fx.away_goals : "-"}
                </td>

                {/* ASISTENCIA - SOLO EL NÚMERO */}
                <td style={{
                  padding: '10px 8px',
                  textAlign: 'right',
                  fontVariantNumeric: 'tabular-nums',
                  verticalAlign: 'middle'
                }}>
                  {attendanceDisplay}
                </td>

                {/* ESTADIO - SOLO EL NOMBRE */}
                <td style={{
                  padding: '10px 8px',
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  verticalAlign: 'middle'
                }}>
                  {fx.venue || "-"}
                </td>

                {/* ÁRBITRO - SOLO EL NOMBRE */}
                <td style={{
                  padding: '10px 8px',
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  verticalAlign: 'middle'
                }}>
                  {fx.referee || "-"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ScorersPage({ season, stage, teamMap }) {
  const scorers = useScorers(season, stage);
  return (
    <section className="section">
      <SectionHeader title="Goleadores" subtitle="Top 20 por torneo" />
      {scorers.loading && <p>Cargando goleadores...</p>}
      {scorers.error && <p className="error">{scorers.error}</p>}
      {!scorers.loading && scorers.items.length > 0 && <ScorersTable scorers={scorers.items} teamMap={teamMap} />}
    </section>
  );
}

function ScorersTable({ scorers, teamMap }) {
  return (
    <div className="table-wrapper">
      <table className="data-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Jugador</th>
            <th>Equipo</th>
            <th>Goles</th>
          </tr>
        </thead>
        <tbody>
          {scorers.map((row, idx) => (
            <tr key={`${row.player}-${row.team}`}>
              <td>{idx + 1}</td>
              <td>{row.player}</td>
              <td className="team-cell">
                {teamMap?.[row.team_id]?.logo_key && <Logo logoKey={teamMap[row.team_id].logo_key} alt={row.team} />}
                <span className="team-name" title={row.team}>{row.team}</span>
              </td>
              <td>{row.goals}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function InsightsPage({ season, stage, teams }) {
  const insights = useInsights(season, stage);
  const shortNames = useMemo(
    () => Object.fromEntries((teams || []).map((t) => [t.name, t.short_name || t.name])),
    [teams]
  );
  const logoByTeamName = useMemo(
    () => Object.fromEntries((teams || []).map((t) => [t.name, t.logo_key])),
    [teams]
  );
  return (
    <section className="section">
      <SectionHeader title="Gráficos" subtitle="Dashboards separados por métrica" />
      {insights.loading && <p>Cargando...</p>}
      {insights.error && <p className="error">{insights.error}</p>}
      {!insights.loading && insights.data && (
        <div className="stats-grid">
          <StatsCard
            title="Puntos por equipo"
            data={insights.data.points_by_team}
            shortNames={shortNames}
            logoByTeamName={logoByTeamName}
          />
          <StatsCard
            title="Goles a favor por equipo"
            data={insights.data.goals_for_by_team}
            shortNames={shortNames}
            logoByTeamName={logoByTeamName}
          />
          <StatsCard
            title="Tarjetas por equipo"
            data={insights.data.cards_by_team.map((row) => ({
              team: row.team,
              Amarillas: row.yellow,
              Rojas: row.red,
              logo_key: row.logo_key,
            }))}
            bars={[
              { key: "Amarillas", color: "#fcd34d" },
              { key: "Rojas", color: "#f87171" },
            ]}
            shortNames={shortNames}
            logoByTeamName={logoByTeamName}
          />
          <StatsCard
            title="Asistencia promedio"
            data={insights.data.attendance_by_team}
            shortNames={shortNames}
            logoByTeamName={logoByTeamName}
          />
        </div>
      )}
    </section>
  );
}

function StatsCard({ title, data, bars, shortNames, logoByTeamName }) {
  const series = bars || [{ key: "value", color: "#60a5fa" }];
  const safeData = data || [];
  const labelFor = (team) => shortNames?.[team] || team;
  const displayed = useMemo(() => {
    if (!safeData.length) return [];
    const singleKey = series.length === 1 ? series[0].key : null;
    const sorted = [...safeData].sort((a, b) => {
      if (singleKey) {
        return (b?.[singleKey] ?? 0) - (a?.[singleKey] ?? 0);
      }
      const sum = (row) =>
        series.reduce((acc, serie) => acc + (typeof row?.[serie.key] === "number" ? row[serie.key] : 0), 0);
      return sum(b) - sum(a);
    });
    return sorted.slice(0, 8);
  }, [safeData, series]);

  const TeamTick = ({ x, y, payload }) => {
    const team = payload.value;
    const short = labelFor(team);
    const logoKey = logoByTeamName?.[team] ?? displayed.find((row) => row.team === team)?.logo_key;

    return (
      <g transform={`translate(${x},${y})`}>
        {logoKey && (
          <image
            href={`/logos/${logoKey}.png`}
            x={-42}
            y={-9}
            width={18}
            height={18}
            preserveAspectRatio="xMidYMid meet"
          />
        )}
        <text 
          x={logoKey ? -20 : 0} 
          y={4} 
          textAnchor="start" 
          fill="#e5e7eb" 
          fontSize={12}
          fontWeight="500"
        >
          {short}
        </text>
      </g>
    );
  };

  // Calcular dominio máximo para mejor escala
  const maxValue = useMemo(() => {
    if (!displayed.length) return 1;
    const singleKey = series.length === 1 ? series[0].key : null;
    if (singleKey) {
      return Math.max(...displayed.map(item => item[singleKey] || 0));
    }
    return Math.max(...displayed.map(item => 
      series.reduce((sum, serie) => sum + (item[serie.key] || 0), 0)
    ));
  }, [displayed, series]);

  // Función para formatear números (eliminar decimales innecesarios)
  const formatNumber = (value) => {
    if (value === 0) return '0';
    if (value < 1) return value.toFixed(1);
    return Math.round(value).toLocaleString('es-UY');
  };

  return (
    <div className="card" style={{ minHeight: '340px' }}>
      <h3 style={{ margin: '0 0 16px 0', fontSize: '18px', color: '#e5e7eb' }}>{title}</h3>
      <div style={{ width: '100%', height: '280px' }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={displayed}
            layout="vertical"
            margin={{ top: 10, right: 20, bottom: 10, left: 100 }}
            barCategoryGap={10}
            barSize={20}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
            <XAxis
              type="number"
              stroke="#9ca3af"
              tick={{ fill: "#9ca3af", fontSize: 11 }}
              allowDecimals={false}
              domain={[0, maxValue * 1.1]}
              tickFormatter={formatNumber}
            />
            <YAxis
              type="category"
              dataKey="team"
              stroke="transparent"
              tickLine={false}
              axisLine={false}
              tick={<TeamTick />}
              width={95}
              interval={0}
            />
            <Tooltip
              labelFormatter={labelFor}
              formatter={(value, name) => [formatNumber(value), name]}
              contentStyle={{ 
                backgroundColor: '#1f2937', 
                border: '1px solid #374151',
                borderRadius: '8px',
                color: '#f9fafb',
                fontSize: '12px'
              }}
              itemStyle={{ color: '#f9fafb' }}
            />
            <Legend 
              wrapperStyle={{ fontSize: '12px', color: '#9ca3af', paddingTop: '10px' }}
            />
            {series.map((serie) => (
              <Bar 
                key={serie.key} 
                dataKey={serie.key} 
                fill={serie.color}
                radius={[0, 4, 4, 0]}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function PlayersPage({ season, stage, teams, teamMap }) {
  const [teamFilter, setTeamFilter] = useState("");
  const stats = usePlayerStats(season, stage, teamFilter || undefined);
  return (
    <section className="section">
      <SectionHeader
        title="Standard stats"
        subtitle="Tabla estilo FBref para jugadores"
        extra={
          <Selector
            label="Equipo"
            value={teamFilter}
            options={[{ value: "", label: "Todos" }, ...teams.map((t) => ({ value: t.id, label: t.name }))]}
            onChange={setTeamFilter}
          />
        }
      />
      {stats.loading && <p>Cargando...</p>}
      {stats.error && <p className="error">{stats.error}</p>}
      {!stats.loading && stats.rows.length > 0 && <PlayersTable rows={stats.rows} teamMap={teamMap} />}
    </section>
  );
}

function PlayersTable({ rows, teamMap }) {
  return (
    <div className="table-wrapper">
      <table className="data-table players-table">
        <thead>
          <tr>
            <th>Jugador</th>
            <th>Equipo</th>
            <th>Nación</th>
            <th>Pos</th>
            <th>Edad</th>
            <th>MP</th>
            <th>Starts</th>
            <th>Min</th>
            <th>Gls</th>
            <th>Ast</th>
            <th>Sh</th>
            <th>SoT</th>
            <th>CrdY</th>
            <th>CrdR</th>
            <th>xG</th>
            <th>xA</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.player_id}-${row.team_id}`}>
              <td className="player-cell">
                <span className="player-name" title={row.full_name || row.player}>{row.full_name || row.player}</span>
              </td>
              <td className="team-cell">
                {teamMap?.[row.team_id]?.logo_key && <Logo logoKey={teamMap[row.team_id].logo_key} alt={row.team} />}
                <span className="team-name" title={row.team}>{row.team}</span>
              </td>
              <td className="nation-cell">{iso2ToFlag(row.nationality_iso2 || row.nation)}</td>
              <td>{row.position || row.pos}</td>
              <td>{row.age ?? "-"}</td>
              <td>{row.mp}</td>
              <td>{row.starts}</td>
              <td>{row.minutes ?? row.min ?? 0}</td>
              <td>{row.goals ?? row.gls ?? 0}</td>
              <td>{row.assists ?? row.ast ?? 0}</td>
              <td>{row.shots ?? row.sh ?? 0}</td>
              <td>{row.shots_on_target ?? row.sot ?? 0}</td>
              <td>{row.yellows ?? row.crdy ?? 0}</td>
              <td>{row.reds ?? row.crdr ?? 0}</td>
              <td>{row.xg ?? 0}</td>
              <td>{row.xa ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AiConsultorPage({ season, stage, teams }) {
  const [form, setForm] = useState({ a: "", b: "" });
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

async function runAdvisor(e) {
  e.preventDefault();
  if (!form.a || !form.b) {
    setError("Elegí dos equipos");
    return;
  }
  setError("");
  setLoading(true);
  setResult(null);
  try {
    const res = await fetch(`${API_BASE}/ai/consultor`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        teamA_id: Number(form.a),
        teamB_id: Number(form.b),
        season,
        stage,
      }),
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || "Error consultor");
    }
    
    // Adaptar la respuesta nueva al formato esperado por el frontend
    if (data.respuesta) {
      // Es la nueva respuesta conversacional
      setResult({
        recomendacion: data.respuesta,
        equipo_a: data.equipo_a || { team: form.a },
        equipo_b: data.equipo_b || { team: form.b }
      });
    } else {
      // Es la respuesta vieja
      setResult(data);
    }
  } catch (err) {
    setError(err.message || "Error consultor");
  } finally {
    setLoading(false);
  }
}

  return (
    <section className="section">
      <SectionHeader title="AI Consultor" subtitle="Resumen cuantitativo simple" />
      <form className="form" onSubmit={runAdvisor}>
        <Selector
          label="Equipo A"
          value={form.a}
          options={teams.map((t) => ({ value: t.id, label: t.name }))}
          onChange={(v) => setForm((prev) => ({ ...prev, a: v }))}
        />
        <Selector
          label="Equipo B"
          value={form.b}
          options={teams.map((t) => ({ value: t.id, label: t.name }))}
          onChange={(v) => setForm((prev) => ({ ...prev, b: v }))}
        />
        <button type="submit" className="primary" disabled={loading}>
          {loading ? "Consultando..." : "Consultar"}
        </button>
        <button type="button" className="ghost" onClick={() => navigate("/tables")}>Volver a tablas</button>
      </form>
      {error && <p className="error">{error}</p>}
      {result && (
        <div className="card">
          <h4>
            {result.equipo_a.team} vs {result.equipo_b.team}
          </h4>
          <p>{result.recomendacion}</p>
          <div className="two-col">
            <TeamMiniSummary title="Equipo A" data={result.equipo_a} />
            <TeamMiniSummary title="Equipo B" data={result.equipo_b} />
          </div>
        </div>
      )}
    </section>
  );
}

function TeamMiniSummary({ title, data }) {
  if (!data) return null;
  return (
    <div className="mini-card">
      <h5>{title}: {data.team}</h5>
      <ul>
        <li>Pts: {data.pts}</li>
        <li>DG: {data.dg}</li>
        <li>GF/GC: {data.gf}/{data.gc}</li>
        <li>Disciplina: {data.amarillas}A / {data.rojas}R</li>
        <li>Racha: {data.last5 || "-"}</li>
      </ul>
    </div>
  );
}

function ConsultasLibresPage({ season, stage, teams }) {
  const [consulta, setConsulta] = useState("");
  const [resultado, setResultado] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function enviarConsulta(e) {
    e.preventDefault();
    if (!consulta.trim()) {
      setError("Escribí una pregunta");
      return;
    }
    setError("");
    setLoading(true);
    setResultado(null);
    
    try {
      const res = await fetch(`${API_BASE}/ai/consulta-libre`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          consulta: consulta,
          season,
          stage
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.detail || "Error en consulta");
      }
      setResultado(data);
    } catch (err) {
      setError(err.message || "Error en consulta");
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="section">
      <SectionHeader 
        title="Consultas Libres IA" 
        subtitle="Hacé cualquier pregunta sobre el torneo uruguayo"
      />
      
      <form onSubmit={enviarConsulta} className="form">
        <div style={{flex: 1}}>
          <label className="selector">
            <span>Tu pregunta:</span>
            <input 
              type="text" 
              value={consulta}
              onChange={(e) => setConsulta(e.target.value)}
              placeholder="Ej: ¿Cómo va Peñarol? ¿Quién es el goleador? Compara Nacional vs Liverpool"
              style={{
                width: "100%",
                padding: "0.5rem",
                background: "#0f172a",
                color: "#e5e7eb",
                border: "1px solid #1f2937",
                borderRadius: "10px"
              }}
            />
          </label>
        </div>
        <button type="submit" className="primary" disabled={loading}>
          {loading ? "Consultando..." : "Preguntar"}
        </button>
      </form>

      {error && <p className="error">{error}</p>}
      
      {resultado && (
        <div className="card">
          <h4>Respuesta:</h4>
          <p style={{whiteSpace: "pre-line"}}>{resultado.respuesta}</p>
          <div className="muted" style={{marginTop: "1rem", fontSize: "0.8rem"}}>
            Intención detectada: {JSON.stringify(resultado.intencion_detectada)}
          </div>
        </div>
      )}

      <div className="card" style={{marginTop: "1rem"}}>
        <h5>Ejemplos de preguntas:</h5>
        <ul>
          <li>¿Cómo va Peñarol en el torneo?</li>
          <li>Compara Nacional vs Liverpool</li>
          <li>¿Quién es el goleador del apertura?</li>
          <li>Mostrá la tabla de posiciones</li>
          <li>¿Cómo le está yendo a Boston River?</li>
          <li>Dame los datos de Danubio</li>
        </ul>
      </div>
    </section>
  );
}

function Selector({ label, value, options, onChange }) {
  return (
    <label className="selector">
      <span>{label}</span>
      <select value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function SectionHeader({ title, subtitle, extra }) {
  return (
    <div className="section-header">
      <div>
        <h2>{title}</h2>
        <p className="muted">{subtitle}</p>
      </div>
      {extra}
    </div>
  );
}

function App() {
  const [meta, setMeta] = useState(null);
  const [season, setSeason] = useState(null);
  const [stage, setStage] = useState(null);
  const [metaError, setMetaError] = useState("");

  useEffect(() => {
    fetchJson(`/meta`)
      .then((data) => {
        setMeta(data);
        setSeason(data.default_season);
        setStage(data.default_stage);
      })
      .catch((err) => setMetaError(err.message || "No se pudo cargar metadata"));
  }, []);

  const teams = meta?.teams ?? [];
  const seasons = meta?.seasons ?? [];
  const stages = meta?.stages ?? [];
  const teamMap = useMemo(() => Object.fromEntries(teams.map((t) => [t.id, t])), [teams]);

  if (metaError) {
    return <div className="app">Error al cargar metadata: {metaError}</div>;
  }
  if (!meta) {
    return <div className="app">Cargando metadata...</div>;
  }

  return (
    <div className="app">
      <Header
        seasons={seasons}
        stages={stages}
        season={season}
        stage={stage}
        onSeasonChange={setSeason}
        onStageChange={setStage}
      />
      <Navigation />
      <main className="content">
        <Routes>
          <Route path="/" element={<Navigate to="/tables" replace />} />
          <Route path="/tables" element={<TablesPage season={season} stage={stage} onStageChange={setStage} />} />
          <Route path="/fixtures" element={<FixturesPage season={season} stage={stage} teams={teams} />} />
          <Route path="/scorers" element={<ScorersPage season={season} stage={stage} teamMap={teamMap} />} />
          <Route path="/stats" element={<InsightsPage season={season} stage={stage} teams={teams} />} />
          <Route path="/players" element={<PlayersPage season={season} stage={stage} teams={teams} teamMap={teamMap} />} />
          <Route path="/ai-consultor" element={<AiConsultorPage season={season} stage={stage} teams={teams} />} />
          <Route path="/consultas-libres" element={<ConsultasLibresPage season={season} stage={stage} teams={teams} />} />
        </Routes>
      </main>
    </div>
  );
}

export default App;